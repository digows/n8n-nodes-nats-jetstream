const assert = require('node:assert/strict');
const { test } = require('node:test');
const { connect, headers } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, StorageType } = require('@nats-io/jetstream');
const { broker, until, triggerContext, run } = require('../helpers.cjs');
const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node');

function failedNodeRun()
{
    const error = { name: 'NodeOperationError', message: 'Synthetic terminal node failure', stack: 'synthetic' };
    return { ...run('error'), stoppedAt: new Date(), data: { resultData: { error, lastNodeExecuted: 'Fail', runData: { Fail: [{ executionStatus: 'error', error }] } } } };
}

test('real broker batching and execution-error policies', async suite =>
{
    const server = await broker();
    const connection = await connect({ servers: server.url });
    const manager = await jetstreamManager(connection), client = jetstream(connection);
    await manager.streams.add({ name: 'BATCH', subjects: ['batch.>'], storage: StorageType.File });
    suite.diagnostic(`Actual broker ${connection.info.version}`);
    const credential = { servers: server.url, authentication: 'none' };
    const parameters = (durable, options = {}, format = 'text') => ({
        stream: 'BATCH', durable, consumerMode: 'create', payloadFormat: format,
        creation: { subjectFilters: `batch.${durable}`, ackWaitMs: 150, maxAckPending: -1 },
        options: { batchSize: 2, maxInFlight: 4, pullTimeoutMs: 1000, nakDelayMs: 50, shutdownGraceMs: 1000, ...options },
    });
    try
    {
        await suite.test('one ordered batch at a time, lossless text and progress through collection and completion', async () =>
        {
            const emitted = [];
            const context = triggerContext(credential, parameters('serial', { onExecutionError: 'acknowledge' }), (items, _response, completion) => emitted.push({ items: items[0], completion }));
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            const values = Array.from({ length: 5 }, (_, index) => `{"index":${index},"large":9223372036854775807,"text":"original\\u0000"}`);
            try
            {
                const applicationHeaders = headers();
                applicationHeaders.append('X-Repeated', 'one'); applicationHeaders.append('X-Repeated', 'two');
                for (const [index, value] of values.entries()) await client.publish('batch.serial', Buffer.from(value), { headers: applicationHeaders, msgID: `serial-${index}` });
                await until(() => emitted.length === 1);
                await new Promise(resolve => setTimeout(resolve, 450));
                const busy = await manager.consumers.info('BATCH', 'serial');
                assert.equal(busy.num_ack_pending, 2); assert.equal(busy.num_pending, 3); assert.equal(busy.num_redelivered, 0);
                assert.equal(emitted.length, 1, 'Extra message budget must not allow another concurrent batch');
                assert.deepEqual(emitted[0].items.map(item => item.json.payload), values.slice(0, 2));
                assert.deepEqual(emitted[0].items.map(item => item.pairedItem.item), [0, 1]);
                assert.deepEqual(emitted[0].items[0].json.metadata.headers['X-Repeated'], ['one', 'two']);
                assert.equal(emitted[0].items[0].json.metadata.messageId, 'serial-0');
                emitted[0].completion.resolve(run('success'));
                await until(() => emitted.length === 2);
                assert.deepEqual(emitted[1].items.map(item => item.json.payload), values.slice(2, 4));
                const failed = failedNodeRun(), original = structuredClone(failed);
                emitted[1].completion.resolve(failed);
                await until(async () => (await manager.consumers.info('BATCH', 'serial')).num_ack_pending === 1);
                await new Promise(resolve => setTimeout(resolve, 450));
                assert.equal((await manager.consumers.info('BATCH', 'serial')).num_redelivered, 0, 'Buffered tail needs progress while the bounded collection waits');
                await until(() => emitted.length === 3);
                assert.deepEqual(emitted[2].items.map(item => item.json.payload), values.slice(4));
                emitted[2].completion.resolve(run('waiting'));
                await until(async () => (await manager.consumers.info('BATCH', 'serial')).num_ack_pending === 0);
                assert.deepEqual(failed, original, 'Transport settlement must not rewrite the failed n8n result');
                assert.deepEqual(context.fatal, []); assert.deepEqual(context.failures, []);
            }
            finally { await response.closeFunction(); }
        });
        await suite.test('default errors and rejected completion redeliver; opt-in ACK includes markerless and operational returned errors', async () =>
        {
            for (const scenario of [
                { durable: 'default-error', result: failedNodeRun() },
                { durable: 'node-error', acknowledge: true, result: failedNodeRun() },
                { durable: 'markerless-code', acknowledge: true, result: { status: 'error', data: { resultData: { runData: {}, error: { message: 'Synthetic Code failure' } } } } },
                { durable: 'operational-error', acknowledge: true, result: { status: 'error', data: { resultData: { error: { message: 'Synthetic persistence failure' } } } } },
                { durable: 'rejected-completion', acknowledge: true, reject: true },
            ])
            {
                const emitted = [];
                const failed = scenario.result;
                const original = structuredClone(failed);
                const context = triggerContext(credential, parameters(scenario.durable, scenario.acknowledge ? { onExecutionError: 'acknowledge' } : {}), (items, _response, completion) =>
                {
                    emitted.push(items[0]);
                    if (scenario.reject && emitted.length === 1) { completion.reject(new Error('Synthetic rejected completion')); return; }
                    completion.resolve(emitted.length === 1 ? failed : run('success'));
                });
                const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
                try
                {
                    let published;
                    for (let index = 0; index < 2; index++) published = await client.publish(`batch.${scenario.durable}`, Buffer.from(String(index)));
                    if (scenario.acknowledge && !scenario.reject)
                    {
                        await until(async () =>
                        {
                            const consumer = await manager.consumers.info('BATCH', scenario.durable);
                            return emitted.length === 1 && consumer.ack_floor.stream_seq >= published.seq && consumer.num_ack_pending === 0;
                        });
                        await new Promise(resolve => setTimeout(resolve, 300));
                        assert.equal(emitted.length, 1);
                        assert.equal((await manager.consumers.info('BATCH', scenario.durable)).num_redelivered, 0);
                    }
                    else
                    {
                        await until(() => emitted.length === 2);
                        assert.ok(emitted[1].every(item => item.json.metadata.redelivered));
                        await until(async () => (await manager.consumers.info('BATCH', scenario.durable)).num_ack_pending === 0);
                    }
                    assert.deepEqual(failed, original);
                    assert.deepEqual(context.fatal, []);
                }
                finally { await response.closeFunction(); }
            }
        });
        await suite.test('manual batch closes its listener before completion and leaves later messages pending', async () =>
        {
            let completion;
            let executionCount = 0;
            const context = triggerContext(credential, parameters('manual', { maxInFlight: 2, shutdownGraceMs: 2000 }), (items, _response, done) =>
            {
                executionCount++; assert.equal(items[0].length, 2); completion = done;
            });
            context.getMode = () => 'manual';
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            try
            {
                await response.manualTriggerFunction();
                for (let index = 0; index < 4; index++) await client.publish('batch.manual', Buffer.from(String(index)));
                await until(() => completion);
                await response.closeFunction();
                assert.equal((await manager.consumers.info('BATCH', 'manual')).num_ack_pending, 2);
                completion.resolve(run('success'));
                await until(async () => await server.connections() === 1);
                const settled = await manager.consumers.info('BATCH', 'manual');
                assert.equal(settled.num_ack_pending, 0); assert.equal(settled.num_pending, 2); assert.equal(executionCount, 1);
            }
            finally { completion?.resolve(run('success')); await response.closeFunction(); }
        });
        await suite.test('manual batching keeps listening after an empty bounded pull', async () =>
        {
            let completion;
            const context = triggerContext(credential, parameters('manual-empty'), (_items, _response, done) => { completion = done; });
            context.getMode = () => 'manual';
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            try
            {
                await response.manualTriggerFunction();
                await new Promise(resolve => setTimeout(resolve, 1200));
                assert.equal(completion, undefined);
                for (let index = 0; index < 2; index++) await client.publish('batch.manual-empty', Buffer.from(String(index)));
                await until(() => completion);
                await response.closeFunction();
                completion.resolve(run('success'));
                await until(async () => await server.connections() === 1);
                assert.equal((await manager.consumers.info('BATCH', 'manual-empty')).num_ack_pending, 0);
            }
            finally { completion?.resolve(run('success')); await response.closeFunction(); }
        });
        await suite.test('shutdown during partial collection never emits or success-ACKs buffered work', async () =>
        {
            let executionCount = 0;
            const context = triggerContext(credential, parameters('collect-close', { batchSize: 3, maxInFlight: 3, shutdownGraceMs: 50 }), () => executionCount++);
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            await client.publish('batch.collect-close', Buffer.from('buffered'));
            await until(async () => (await manager.consumers.info('BATCH', 'collect-close')).num_ack_pending === 1);
            await response.closeFunction();
            assert.equal(executionCount, 0);
            const consumer = await client.consumers.get('BATCH', 'collect-close');
            const recovered = await consumer.next({ expires: 1000 });
            assert.ok(recovered); assert.equal(recovered.redelivered, true); assert.equal(await recovered.ackAck(), true);
        });
        await suite.test('shutdown releases an unfinished execution batch and late success does not reacquire its ACKs', async () =>
        {
            let completion;
            const context = triggerContext(credential, parameters('run-close', { shutdownGraceMs: 30, onExecutionError: 'acknowledge' }), (_items, _response, done) => { completion = done; });
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            for (let index = 0; index < 2; index++) await client.publish('batch.run-close', Buffer.from(String(index)));
            await until(() => completion);
            await response.closeFunction();
            const consumer = await client.consumers.get('BATCH', 'run-close');
            const recovered = await consumer.fetch({ max_messages: 2, expires: 1000 });
            let count = 0;
            for await (const message of recovered) { count++; assert.equal(message.redelivered, true); assert.equal(await message.ackAck(), true); }
            assert.equal(count, 2);
            completion.resolve(run('success'));
            await new Promise(resolve => setTimeout(resolve, 50));
            assert.equal((await manager.consumers.info('BATCH', 'run-close')).num_ack_pending, 0);
        });
        await suite.test('binary batches preserve each message through the existing n8n binary helper', async () =>
        {
            let received;
            const context = triggerContext(credential, parameters('binary', {}, 'binary'), (items, _response, completion) => { received = items[0]; completion.resolve(run('success')); });
            const response = await NatsJetstreamTrigger.prototype.trigger.call(context);
            try
            {
                await client.publish('batch.binary', Buffer.from([0, 1, 255]));
                await client.publish('batch.binary', Buffer.from([2, 0, 254]));
                await until(async () => received && (await manager.consumers.info('BATCH', 'binary')).num_ack_pending === 0);
                assert.deepEqual(received.map(item => [...Buffer.from(item.binary.data.data, 'base64')]), [[0, 1, 255], [2, 0, 254]]);
                assert.ok(received.every(item => item.json.payload === undefined));
                assert.deepEqual(received.map(item => item.pairedItem.item), [0, 1]);
            }
            finally { await response.closeFunction(); }
        });
        await until(async () => await server.connections() === 1);
    }
    finally { await connection.close(); await server.close(); }
});
