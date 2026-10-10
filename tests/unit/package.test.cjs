const assert = require('node:assert/strict');
const { test } = require('node:test');
const { headers } = require('@nats-io/transport-node');
const { connectionOptions } = require('../../dist/lib/connection');
const { creationConfiguration, validateConsumer, progressInterval } = require('../../dist/lib/consumer');
const { messageItem, publishHeaders, publishPayload, publishSubject, sequenceString } = require('../../dist/lib/payload');
const { completedBoundary, deliverMessage } = require('../../dist/lib/delivery');

function run(status, error)
{
    return { status, waitTill: status === 'waiting' ? new Date(Date.now() + 10000) : undefined, data: { resultData: { error, runData: {} } } };
}

function message(data = '{}')
{
    const messageHeaders = headers();
    messageHeaders.append('X-Test', 'first');
    messageHeaders.append('X-Test', 'second');
    return {
        data: Buffer.from(data), subject: 'tests.one', timestamp: '2026-10-09T00:00:00.000Z', headers: messageHeaders,
        redelivered: false, info: { stream: 'TESTS', consumer: 'handler', streamSequence: 1, deliverySequence: 2, deliveryCount: 1 },
        working() {}, nak() {}, async ackAck() { return true; },
    };
}

function context()
{
    const failures = [];
    return {
        failures, logger: { warn() {} }, getNode: () => ({ name: 'test' }),
        saveFailedExecution(error) { failures.push(error); },
        helpers: {
            createDeferredPromise()
            {
                let resolve, reject;
                const promise = new Promise((success, failure) => { resolve = success; reject = failure; });
                return { promise, resolve, reject };
            },
            async prepareBinaryData(buffer) { return { data: buffer.toString('base64'), mimeType: 'application/octet-stream' }; },
        },
    };
}

test('authentication and TLS are explicit and never fall back', () =>
{
    const base = { servers: 'nats://localhost:4222, tls://localhost:4223', authentication: 'none' };
    assert.deepEqual(connectionOptions(base).servers, ['nats://localhost:4222', 'tls://localhost:4223']);
    for (const authentication of ['token', 'userPassword', 'nkey', 'creds', 'unknown'])
    {
        assert.throws(() => connectionOptions({ ...base, authentication }));
    }
    assert.throws(() => connectionOptions({ ...base, servers: 'nats://user:password@localhost' }));
    assert.throws(() => connectionOptions({ ...base, tls: true, clientCertificate: 'certificate' }));
    assert.throws(() => connectionOptions({ ...base, tls: false, ca: 'certificate' }));
    assert.equal(connectionOptions({ ...base, tls: true }).tls.rejectUnauthorized, true);
});

test('consumer validation and BackOff deadline', () =>
{
    const configuration = creationConfiguration('handler', { subjectFilters: 'a.*, b.>', ackWaitMs: 900 });
    assert.deepEqual(configuration.filter_subjects, ['a.*', 'b.>']);
    assert.equal(configuration.deliver_policy, 'new');
    validateConsumer({ config: configuration }, { durable: 'handler', mode: 'create', configuration });
    assert.throws(() => validateConsumer({ config: { ...configuration, ack_policy: 'all' } }, { durable: 'handler', mode: 'existing' }));
    assert.throws(() => validateConsumer({ config: { ...configuration, deliver_subject: 'push' } }, { durable: 'handler', mode: 'existing' }));
    assert.throws(() => validateConsumer({ config: { ...configuration, max_deliver: 11 } }, { durable: 'handler', mode: 'create', configuration }), /not modified/);
    assert.equal(progressInterval({ config: { ack_wait: 900000000, backoff: [300000000, 600000000] } }), 100);
});

test('payload envelope supports scalar JSON, repeated headers and binary helpers', async () =>
{
    for (const payload of ['null', 'false', '123', '"text"', '[1,2]', '{"metadata":"payload-owned"}'])
    {
        const item = await messageItem(message(payload), 'json', context().helpers);
        assert.deepEqual(item.json.payload, JSON.parse(payload));
        assert.deepEqual(item.json.metadata.headers['X-Test'], ['first', 'second']);
        assert.equal(item.json.metadata.messageId, null);
        assert.equal(item.json.metadata.streamSequence, '1');
    }
    await assert.rejects(messageItem(message('{invalid'), 'json', context().helpers), /not valid JSON/);
    const binaryItem = await messageItem(message('bytes'), 'binary', context().helpers);
    assert.equal(binaryItem.json.payload, undefined);
    assert.equal(Buffer.from(binaryItem.binary.data.data, 'base64').toString(), 'bytes');
    assert.throws(() => sequenceString(Number.MAX_SAFE_INTEGER + 1), /precision/);
});

test('publisher encodes once, preserves headers and refuses reserved conflicts', () =>
{
    assert.equal(Buffer.from(publishPayload({ order: 1 }, 'json')).toString(), '{"order":1}');
    assert.equal(Buffer.from(publishPayload('text', 'text')).toString(), 'text');
    assert.deepEqual(publishHeaders({ 'X-Test': ['one', 'two'] }).values('X-Test'), ['one', 'two']);
    assert.throws(() => publishHeaders({ 'Nats-Msg-Id': 'other' }, 'requested'), /conflicts/);
    assert.throws(() => publishHeaders({ 'Nats-Expected-Stream': 'OTHER' }, undefined, 'EXPECTED'), /conflicts/);
    for (const subject of ['a.*', 'a.>', 'a..b', 'a b', '']) assert.throws(() => publishSubject(subject));
});

test('completion requires a known successful or waiting result, not promise resolution', () =>
{
    assert.equal(completedBoundary(run('success')), true);
    assert.equal(completedBoundary(run('waiting')), true);
    assert.equal(completedBoundary({ ...run('waiting'), waitTill: undefined }), false);
    for (const status of ['error', 'canceled', 'crashed', 'running', 'unknown']) assert.equal(completedBoundary(run(status)), false);
    assert.equal(completedBoundary(run('success', { message: 'error' })), false);
    assert.equal(completedBoundary(undefined), false);
});

test('no ACK on emit; progress spans execution; success and Wait use confirmed ACK', async () =>
{
    for (const status of ['success', 'waiting'])
    {
        const testContext = context();
        const brokerMessage = message();
        let acknowledgementCount = 0, progressCount = 0;
        let completion;
        testContext.emit = (_data, _response, done) => { completion = done; };
        brokerMessage.ackAck = async () => { acknowledgementCount++; return true; };
        brokerMessage.working = () => progressCount++;
        const task = deliverMessage(testContext, brokerMessage, { format: 'json', progressIntervalMs: 5, nakDelayMs: 10, ackTimeoutMs: 100 }, new AbortController().signal, () => true);
        await new Promise(resolve => setTimeout(resolve, 25));
        assert.equal(acknowledgementCount, 0);
        assert.ok(progressCount >= 2);
        completion.resolve(run(status));
        await task;
        assert.equal(acknowledgementCount, 1);
        const settledProgress = progressCount;
        await new Promise(resolve => setTimeout(resolve, 15));
        assert.equal(progressCount, settledProgress);
    }
});

test('failure, rejected admission, unknown outcome and shutdown never ACK', async () =>
{
    for (const outcome of ['error', 'canceled', 'crashed', 'rejected', 'absent', 'shutdown'])
    {
        const testContext = context();
        const brokerMessage = message();
        const controller = new AbortController();
        let acknowledgements = 0, negativeAcknowledgements = 0;
        brokerMessage.ackAck = async () => { acknowledgements++; return true; };
        brokerMessage.nak = delay => { assert.equal(delay, 10); negativeAcknowledgements++; };
        testContext.emit = (_data, _response, completion) =>
        {
            if (outcome === 'rejected') completion.reject(new Error('Admission failed'));
            else if (outcome === 'shutdown') controller.abort();
            else completion.resolve(outcome === 'absent' ? undefined : run(outcome));
        };
        await deliverMessage(testContext, brokerMessage, { format: 'json', progressIntervalMs: 5, nakDelayMs: 10, ackTimeoutMs: 100 }, controller.signal, () => true);
        assert.equal(acknowledgements, 0);
        assert.equal(negativeAcknowledgements, 1);
    }
});

test('malformed payload is recorded and delayed-NAKed; uncertain ACK is not NAKed', async () =>
{
    const testContext = context();
    const brokerMessage = message('{bad');
    let negativeAcknowledgements = 0;
    brokerMessage.nak = () => negativeAcknowledgements++;
    testContext.emit = () => assert.fail('Malformed payload must not emit');
    const options = { format: 'json', progressIntervalMs: 10, nakDelayMs: 10, ackTimeoutMs: 100 };
    await deliverMessage(testContext, brokerMessage, options, new AbortController().signal, () => true);
    assert.equal(testContext.failures.length, 1);
    assert.equal(negativeAcknowledgements, 1);
    const uncertain = message();
    testContext.emit = (_data, _response, completion) => completion.resolve(run('success'));
    uncertain.ackAck = async () => { throw new Error('Timeout'); };
    uncertain.nak = () => assert.fail('Uncertain ACK must not NAK');
    await deliverMessage(testContext, uncertain, options, new AbortController().signal, () => true);
    assert.equal(testContext.failures.length, 2);
});
