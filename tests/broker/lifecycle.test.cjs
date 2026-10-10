const assert = require('node:assert/strict');
const { test } = require('node:test');
const { fork } = require('node:child_process');
const { join } = require('node:path');
const { connect } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, AckPolicy, DeliverPolicy, StorageType } = require('@nats-io/jetstream');
const { broker, until, triggerContext, run } = require('../helpers.cjs');
const { bindConsumer, creationConfiguration } = require('../../dist/lib/consumer');
const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node');

test('overlapping trigger processes distribute deliveries; crash recovery preserves the durable cursor', async () =>
{
    const server = await broker();
    const connection = await connect({ servers: server.url });
    const manager = await jetstreamManager(connection), client = jetstream(connection);
    const processes = [];
    try
    {
        await manager.streams.add({ name: 'LIFECYCLE', subjects: ['lifecycle.>'], storage: StorageType.File });
        await manager.consumers.add('LIFECYCLE', { durable_name: 'shared', ack_policy: AckPolicy.Explicit, ack_wait: 200000000, filter_subject: 'lifecycle.shared' });
        for (let index = 0; index < 2; index++)
        {
            const child = fork(join(__dirname, '../fixtures/trigger-process.cjs'), [], { env: { ...process.env, TEST_NATS_URL: server.url }, stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
            const messages = [];
            child.on('message', message => messages.push(message));
            processes.push({ child, messages });
            await until(() => messages.some(message => message.type === 'ready'));
        }
        await client.publish('lifecycle.shared', Buffer.from('{}'));
        await client.publish('lifecycle.shared', Buffer.from('{}'));
        await until(() => processes.every(process => process.messages.some(message => message.type === 'delivery')));
        const initial = processes.map(process => process.messages.find(message => message.type === 'delivery').metadata);
        assert.notEqual(initial[0].streamSequence, initial[1].streamSequence);
        await new Promise(resolve => setTimeout(resolve, 600));
        assert.equal((await manager.consumers.info('LIFECYCLE', 'shared')).num_redelivered, 0);
        processes[0].child.kill('SIGKILL');
        await new Promise(resolve => processes[0].child.once('exit', resolve));
        processes[1].child.send({ type: 'complete', sequence: initial[1].streamSequence });
        const recovered = await until(() => processes[1].messages.find(message => message.type === 'delivery' && message.metadata.streamSequence === initial[0].streamSequence));
        assert.equal(recovered.metadata.redelivered, true);
        processes[1].child.send({ type: 'complete', sequence: recovered.metadata.streamSequence });
        await until(async () => (await manager.consumers.info('LIFECYCLE', 'shared')).num_ack_pending === 0);
        assert.equal((await manager.consumers.list('LIFECYCLE').next()).length, 1);
        processes[1].child.send({ type: 'close' });
        await new Promise(resolve => processes[1].child.once('exit', resolve));
        await until(async () => await server.connections() === 1);
    }
    finally
    {
        for (const { child } of processes)
        {
            if (child.exitCode === null && child.signalCode === null)
            {
                child.kill('SIGKILL'); await new Promise(resolve => child.once('exit', resolve));
            }
        }
        await connection.close(); await server.close();
    }
});

test('broker restart reconnects without creating another consumer; repeated manual lifecycles release connections', async () =>
{
    const server = await broker();
    const connection = await connect({ servers: server.url, maxReconnectAttempts: -1, reconnectTimeWait: 50 });
    let response;
    try
    {
        const manager = await jetstreamManager(connection), client = jetstream(connection);
        await manager.streams.add({ name: 'LIFECYCLE', subjects: ['lifecycle.>'], storage: StorageType.File });
        const parameters = { stream: 'LIFECYCLE', durable: 'reconnect', consumerMode: 'create', creation: { subjectFilters: 'lifecycle.reconnect', ackWaitMs: 300 }, payloadFormat: 'json', options: { shutdownGraceMs: 100, pullTimeoutMs: 1000 } };
        const deliveries = [];
        const context = triggerContext({ servers: server.url, authentication: 'none' }, parameters, (items, _response, completion) => { deliveries.push(items[0][0].json.metadata); completion.resolve(run('success')); });
        response = await NatsJetstreamTrigger.prototype.trigger.call(context);
        await client.publish('lifecycle.reconnect', Buffer.from('{}'));
        await until(() => deliveries.length === 1);
        await until(async () => (await manager.consumers.info('LIFECYCLE', 'reconnect')).num_ack_pending === 0);
        await server.restart();
        try { await until(async () => await server.connections() === 2, 15000); }
        catch (error) { error.stack += `\nConnections: ${await server.connections()}; fatal: ${context.fatal.map(error => error.message).join('; ')}; ${server.output}`; throw error; }
        await client.publish('lifecycle.reconnect', Buffer.from('{}'));
        await until(() => deliveries.length === 2, 15000);
        assert.equal(deliveries[1].streamSequence, '2'); assert.deepEqual(context.fatal, []);
        await response.closeFunction(); response = undefined;
        await until(async () => await server.connections() === 1);
        for (let index = 0; index < 3; index++)
        {
            const manualContext = triggerContext({ servers: server.url, authentication: 'none' }, { ...parameters, consumerMode: 'existing' }, (_items, _response, completion) => completion.resolve(run('success')));
            manualContext.getMode = () => 'manual';
            response = await NatsJetstreamTrigger.prototype.trigger.call(manualContext);
            await client.publish('lifecycle.reconnect', Buffer.from('{}'));
            await response.manualTriggerFunction();
            await until(async () => { const info = await manager.consumers.info('LIFECYCLE', 'reconnect'); return info.num_ack_pending === 0 && info.num_pending === 0; });
            await response.closeFunction(); response = undefined;
            await until(async () => await server.connections() === 1);
        }
        assert.equal((await manager.consumers.list('LIFECYCLE').next()).length, 1);
        await assert.rejects(NatsJetstreamTrigger.prototype.trigger.call(triggerContext({ servers: server.url, authentication: 'none' }, { ...parameters, durable: 'absent', consumerMode: 'existing' }, () => {})));
        await until(async () => await server.connections() === 1);
    }
    finally { await response?.closeFunction(); await connection.close(); await server.close(); }
});

test('nats-server 2.9 rejects unsafe creation/multiple filters while existing binding remains non-mutating', async () =>
{
    assert.ok(process.env.NATS_OLD_SERVER_BIN, 'Set NATS_OLD_SERVER_BIN to nats-server 2.9.25');
    const server = await broker('', process.env.NATS_OLD_SERVER_BIN);
    const connection = await connect({ servers: server.url });
    try
    {
        const manager = await jetstreamManager(connection);
        await manager.streams.add({ name: 'OLD', subjects: ['old.>'], storage: StorageType.File });
        await assert.rejects(bindConsumer(connection, { stream: 'OLD', durable: 'filters', mode: 'create', configuration: creationConfiguration('filters', { subjectFilters: 'old.a,old.b' }) }), /multiple|filter_subjects|2\.10/i);
        assert.deepEqual(await manager.consumers.list('OLD').next(), []);
        await assert.rejects(bindConsumer(connection, { stream: 'OLD', durable: 'single', mode: 'create', configuration: creationConfiguration('single', { subjectFilters: 'old.a' }) }), /create-only safety/);
        assert.deepEqual(await manager.consumers.list('OLD').next(), []);
        await manager.consumers.add('OLD', { durable_name: 'existing', ack_policy: AckPolicy.Explicit, filter_subject: 'old.a', max_deliver: 10 });
        const { info } = await bindConsumer(connection, { stream: 'OLD', durable: 'existing', mode: 'existing' });
        assert.equal(info.config.max_deliver, 10);
        assert.equal((await manager.consumers.list('OLD').next()).length, 1);
    }
    finally { await connection.close(); await server.close(); }
});
