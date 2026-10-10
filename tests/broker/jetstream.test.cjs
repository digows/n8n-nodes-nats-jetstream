const assert = require('node:assert/strict');
const { test } = require('node:test');
const { connect } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, AckPolicy, StorageType, DeliverPolicy } = require('@nats-io/jetstream');
const { broker, until, triggerContext, run } = require('../helpers.cjs');
const { bindConsumer, creationConfiguration } = require('../../dist/lib/consumer');
const { openConnection } = require('../../dist/lib/connection');
const { JetstreamNatsTrigger } = require('../../dist/nodes/JetstreamNats/JetstreamNatsTrigger.node');
const { JetstreamNats } = require('../../dist/nodes/JetstreamNats/JetstreamNats.node');

test('real JetStream creation, consumption and publishing', async suite =>
{
    const server = await broker();
    const connection = await connect({ servers: server.url });
    const manager = await jetstreamManager(connection);
    const client = jetstream(connection);
    await manager.streams.add({ name: 'TESTS', subjects: ['tests.>'], storage: StorageType.File });
    const credential = { servers: server.url, authentication: 'none' };
    try
    {
        await suite.test('create-only concurrent activation and mismatch preservation', async () =>
        {
            const selection = { stream: 'TESTS', durable: 'creation', mode: 'create', configuration: creationConfiguration('creation', { subjectFilters: 'tests.a, tests.b', deliverPolicy: 'all' }) };
            await Promise.all([bindConsumer(connection, selection), bindConsumer(connection, selection)]);
            await assert.rejects(bindConsumer(connection, { ...selection, configuration: { ...selection.configuration, max_deliver: 999 } }), /not modified/);
            const info = await manager.consumers.info('TESTS', 'creation');
            assert.equal(info.config.max_deliver, 10);
            const races = await Promise.allSettled([
                bindConsumer(connection, { ...selection, durable: 'race', configuration: { ...selection.configuration, durable_name: 'race' } }),
                bindConsumer(connection, { ...selection, durable: 'race', configuration: { ...selection.configuration, durable_name: 'race', max_deliver: 11 } }),
            ]);
            assert.equal(races.filter(result => result.status === 'fulfilled').length, 1);
            const failedRace = races.find(result => result.status === 'rejected');
            assert.match(failedRace.reason.message, /not modified/, `Unexpected create race error: ${failedRace.reason.code} ${failedRace.reason.status} ${failedRace.reason.message}`);
            for (const subject of ['tests.a', 'tests.b', 'tests.c']) await client.publish(subject, Buffer.from('{}'));
            const { consumer } = await bindConsumer(connection, { stream: 'TESTS', durable: 'creation', mode: 'existing' });
            const messages = await consumer.fetch({ max_messages: 2, expires: 1000 });
            const subjects = [];
            for await (const message of messages) { subjects.push(message.subject); assert.equal(await message.ackAck(), true); }
            assert.deepEqual(subjects.sort(), ['tests.a', 'tests.b']);
        });
        await suite.test('publisher JSON/Text/Binary, pairing, repeated headers, duplicate and expected stream', async () =>
        {
            const inputs = [{}, {}, {}];
            const parameters = [
                { subject: 'tests.publish', payloadFormat: 'json', jsonPayload: '{"value":1}', options: { headers: { 'X-Test': ['one', 'two'] }, messageId: 'stable-1' } },
                { subject: 'tests.publish', payloadFormat: 'text', textPayload: 'text', options: {} },
                { subject: 'tests.publish', payloadFormat: 'binary', binaryProperty: 'data', options: {} },
            ];
            let binaryReads = 0;
            const context = {
                async getCredentials() { return credential; }, getInputData: () => inputs,
                getNodeParameter: (name, index) => parameters[index][name], getNode: () => ({ name: 'publisher' }), continueOnFail: () => false,
                helpers: { async getBinaryDataBuffer(index, field) { assert.equal(index, 2); assert.equal(field, 'data'); binaryReads++; return Buffer.from([0, 1, 255]); } },
            };
            const [results] = await JetstreamNats.prototype.execute.call(context);
            assert.equal(results.length, 3); assert.equal(binaryReads, 1);
            results.forEach((item, index) => assert.equal(item.pairedItem.item, index));
            const duplicateContext = { ...context, getInputData: () => [{}], getNodeParameter: name => parameters[0][name] };
            const [[duplicate]] = await JetstreamNats.prototype.execute.call(duplicateContext);
            assert.equal(duplicate.json.duplicate, true);
            const stored = await manager.streams.getMessage('TESTS', { seq: Number(results[0].json.sequence) });
            assert.equal(Buffer.from(stored.data).toString(), '{"value":1}');
            assert.deepEqual(stored.header.values('X-Test'), ['one', 'two']);
            parameters[0].options.expectedStream = 'WRONG';
            await assert.rejects(JetstreamNats.prototype.execute.call(duplicateContext), /stream/i);
        });
        await suite.test('no ACK on emit, bounded in-flight and progress beyond AckWait', async () =>
        {
            await manager.consumers.add('TESTS', { durable_name: 'progress', ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.New, filter_subject: 'tests.progress', ack_wait: 150000000 });
            const pending = [];
            const context = triggerContext(credential, { stream: 'TESTS', durable: 'progress', consumerMode: 'existing', payloadFormat: 'json', options: { maxInFlight: 2, pullTimeoutMs: 1000, shutdownGraceMs: 1000 } }, (_items, _response, completion) => pending.push(completion));
            const response = await JetstreamNatsTrigger.prototype.trigger.call(context);
            try
            {
                for (let index = 0; index < 5; index++) await client.publish('tests.progress', Buffer.from('{"test":true}'));
                await until(() => pending.length === 2);
                await new Promise(resolve => setTimeout(resolve, 650));
                const info = await manager.consumers.info('TESTS', 'progress');
                assert.equal(info.num_ack_pending, 2); assert.equal(info.num_redelivered, 0); assert.equal(pending.length, 2);
                pending[0].resolve(run('success')); pending[1].resolve(run('waiting'));
                await until(() => pending.length === 4);
                pending[2].resolve(run('success')); pending[3].resolve(run('success'));
                await until(() => pending.length === 5);
                pending[4].resolve(run('success'));
                await until(async () => (await manager.consumers.info('TESTS', 'progress')).num_ack_pending === 0);
                assert.deepEqual(context.fatal, []);
            }
            finally { await response.closeFunction(); }
        });
        await suite.test('failed execution redelivers after delayed NAK', async () =>
        {
            let deliveries = 0;
            const items = [];
            const context = triggerContext(credential, { stream: 'TESTS', durable: 'retry', consumerMode: 'create', creation: { subjectFilters: 'tests.retry' }, payloadFormat: 'json', options: { nakDelayMs: 100, pullTimeoutMs: 1000 } }, (received, _response, completion) =>
            {
                items.push(received[0][0]); completion.resolve(run(++deliveries === 1 ? 'error' : 'success'));
            });
            const response = await JetstreamNatsTrigger.prototype.trigger.call(context);
            try
            {
                await client.publish('tests.retry', Buffer.from('{}'));
                await until(() => deliveries >= 2);
                assert.equal(items[1].json.metadata.redelivered, true);
                await until(async () => (await manager.consumers.info('TESTS', 'retry')).num_ack_pending === 0);
            }
            finally { await response.closeFunction(); }
        });
        await suite.test('shutdown releases unfinished messages without deleting durable; manual receives real data', async () =>
        {
            const received = [];
            const parameters = { stream: 'TESTS', durable: 'shutdown', consumerMode: 'create', creation: { subjectFilters: 'tests.shutdown', ackWaitMs: 100 }, payloadFormat: 'text', options: { shutdownGraceMs: 50, nakDelayMs: 50, pullTimeoutMs: 1000 } };
            const context = triggerContext(credential, parameters, items => received.push(items));
            const response = await JetstreamNatsTrigger.prototype.trigger.call(context);
            await client.publish('tests.shutdown', Buffer.from('real'));
            await until(() => received.length === 1);
            await response.closeFunction();
            assert.equal((await manager.consumers.info('TESTS', 'shutdown')).name, 'shutdown');
            let manualCompletion;
            const manualContext = triggerContext(credential, { ...parameters, consumerMode: 'existing' }, (items, _response, completion) => { assert.equal(items[0][0].json.payload, 'real'); manualCompletion = completion; });
            manualContext.getMode = () => 'manual';
            const manualResponse = await JetstreamNatsTrigger.prototype.trigger.call(manualContext);
            assert.equal(manualCompletion, undefined);
            await manualResponse.manualTriggerFunction();
            await until(() => manualCompletion);
            manualCompletion.resolve(run('success'));
            await until(async () => (await manager.consumers.info('TESTS', 'shutdown')).num_ack_pending === 0);
            await manualResponse.closeFunction();
        });
        await suite.test('native authentication and non-mutating credential test', async () =>
        {
            const protectedServer = await broker('authorization { users: [{user: "tester", password: "test-password"}] }');
            try
            {
                await assert.rejects(openConnection({ servers: protectedServer.url, authentication: 'none' }), /connection failed/);
                const protectedCredentials = { servers: protectedServer.url, authentication: 'userPassword', username: 'tester', password: 'test-password' };
                const result = await new JetstreamNats().methods.credentialTest.testConnection({ data: protectedCredentials });
                assert.equal(result.status, 'OK');
                const invalidResult = await new JetstreamNats().methods.credentialTest.testConnection({ data: { ...protectedCredentials, password: 'wrong', connectionTimeoutMs: 100 } });
                assert.equal(invalidResult.status, 'Error');
                const protectedConnection = await openConnection(protectedCredentials);
                const protectedManager = await jetstreamManager(protectedConnection);
                assert.deepEqual(await protectedManager.streams.list().next(), []);
                await protectedConnection.close();
            }
            finally { await protectedServer.close(); }
        });
    }
    finally { await connection.close(); await server.close(); }
});
