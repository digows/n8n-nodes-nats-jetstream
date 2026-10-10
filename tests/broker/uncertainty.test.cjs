const assert = require('node:assert/strict');
const { test } = require('node:test');
const { connect } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, AckPolicy, StorageType } = require('@nats-io/jetstream');
const { broker, until, triggerContext, run } = require('../helpers.cjs');
const { NatsJetstream } = require('../../dist/nodes/NatsJetstream/NatsJetstream.node');
const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node');

test('publication permissions, lost reply ambiguity and unconfirmed consumer ACK are visible', async () =>
{
    const server = await broker(`authorization { users: [
        {user: "admin", password: "admin-test"},
        {user: "publisher", password: "publisher-test", permissions: { publish: {allow: ["uncertain.allowed"]}, subscribe: {allow: ["_INBOX.>"]} }},
        {user: "lostReply", password: "reply-test", permissions: { publish: {allow: ["uncertain.allowed"]}, subscribe: {deny: ["_INBOX.>"]} }},
        {user: "noAck", password: "ack-test", permissions: { publish: {allow: ["$JS.API.CONSUMER.INFO.UNCERTAIN.noack", "$JS.API.CONSUMER.MSG.NEXT.UNCERTAIN.noack"]}, subscribe: {allow: ["_INBOX.>"]} }}
    ] }`);
    const connection = await connect({ servers: server.url, user: 'admin', pass: 'admin-test' });
    const manager = await jetstreamManager(connection), client = jetstream(connection);
    let response;
    try
    {
        await manager.streams.add({ name: 'UNCERTAIN', subjects: ['uncertain.>'], storage: StorageType.File });
        function publisher(username, password, subject)
        {
            return {
                async getCredentials() { return { servers: server.url, authentication: 'userPassword', username, password }; },
                getInputData: () => [{}], getNode: () => ({ name: 'publisher' }), continueOnFail: () => false,
                getNodeParameter: name => ({ subject, payloadFormat: 'text', textPayload: 'payload', options: { publishTimeoutMs: 100, messageId: 'stable-uncertain' } })[name],
            };
        }
        await assert.rejects(NatsJetstream.prototype.execute.call(publisher('publisher', 'publisher-test', 'uncertain.denied')), /permission|timeout/i);
        const storedBefore = (await manager.streams.info('UNCERTAIN')).state.messages;
        await assert.rejects(NatsJetstream.prototype.execute.call(publisher('lostReply', 'reply-test', 'uncertain.allowed')), /permission|timeout/i);
        const storedAfter = (await manager.streams.info('UNCERTAIN')).state.messages;
        assert.equal(storedAfter, storedBefore + 1, 'Publication can be stored even though its response is lost');
        await manager.consumers.add('UNCERTAIN', { durable_name: 'noack', ack_policy: AckPolicy.Explicit, filter_subject: 'uncertain.noack', ack_wait: 1000000000, max_deliver: 2 });
        const context = triggerContext({ servers: server.url, authentication: 'userPassword', username: 'noAck', password: 'ack-test' }, { stream: 'UNCERTAIN', durable: 'noack', consumerMode: 'existing', payloadFormat: 'text', options: { ackTimeoutMs: 100, shutdownGraceMs: 100, pullTimeoutMs: 1000 } }, (_items, _unused, completion) => completion.resolve(run('success')));
        response = await NatsJetstreamTrigger.prototype.trigger.call(context);
        await client.publish('uncertain.noack', Buffer.from('payload'));
        await until(() => context.failures.length > 0);
        assert.match(context.failures[0].message, /confirmation is unknown/);
        assert.equal((await manager.consumers.info('UNCERTAIN', 'noack')).num_ack_pending, 1);
        await response.closeFunction(); response = undefined;
        await until(async () => await server.connections() === 1);
    }
    finally { await response?.closeFunction(); await connection.close(); await server.close(); }
});
