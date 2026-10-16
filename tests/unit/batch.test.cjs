const assert = require('node:assert/strict');
const { test } = require('node:test');
const { headers } = require('@nats-io/transport-node');
const { deliverBatch, deliverMessage, terminalNodeError } = require('../../dist/lib/delivery');
const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node');
const { triggerContext, run, until } = require('../helpers.cjs');

const options = { format: 'text', progressIntervalMs: 5, nakDelayMs: 10, ackTimeoutMs: 100 };

function delivery(sequence, text = `message-${sequence}`)
{
    const statistics = { progress: 0, acknowledged: 0, released: 0 };
    const messageHeaders = headers();
    messageHeaders.append('X-Repeated', 'one');
    messageHeaders.append('X-Repeated', 'two');
    const message = {
        data: Buffer.from(text), subject: 'batch.test', timestamp: '2026-10-16T00:00:00.000Z', headers: messageHeaders,
        redelivered: false, info: { stream: 'BATCH', consumer: 'test', streamSequence: sequence, deliverySequence: sequence, deliveryCount: 1 },
        working() { statistics.progress++; },
        nak(delay) { assert.equal(delay, options.nakDelayMs); statistics.released++; },
        async ackAck() { statistics.acknowledged++; return true; },
    };
    return { message, statistics };
}

function failedNodeRun()
{
    const error = { name: 'NodeOperationError', message: 'Synthetic node failure', stack: 'synthetic stack' };
    return {
        ...run('error'), stoppedAt: new Date(),
        data: { resultData: { error, lastNodeExecuted: 'Fail', runData: { Fail: [{ executionStatus: 'error', error }] } } },
    };
}

test('terminal error recognition requires matching failed-node evidence, not status alone', () =>
{
    assert.equal(terminalNodeError(failedNodeRun()), true);
    const serialized = JSON.parse(JSON.stringify(failedNodeRun()));
    assert.equal(terminalNodeError(serialized), true);
    for (const mutate of [
        result => { result.stoppedAt = undefined; },
        result => { result.stoppedAt = 'invalid'; },
        result => { result.status = 'canceled'; },
        result => { result.status = 'crashed'; },
        result => { result.data.resultData.runData = {}; },
        result => { result.data.resultData.lastNodeExecuted = undefined; },
        result => { result.data.resultData.runData.Fail[0].executionStatus = 'success'; },
        result => { delete result.data.resultData.runData.Fail[0].error; },
        result => { result.data.resultData.error = { message: 'Persistence failed after node failure' }; },
    ])
    {
        const result = failedNodeRun();
        mutate(result);
        assert.equal(terminalNodeError(result), false);
    }
    assert.equal(terminalNodeError(undefined), false);
});

test('acknowledge policy settles a terminal node error without modifying the failed result', async () =>
{
    for (const policy of ['redeliver', 'acknowledge'])
    {
        const result = failedNodeRun();
        const original = structuredClone(result);
        const selected = delivery(1);
        const context = triggerContext({}, {}, (_items, _response, completion) => completion.resolve(result));
        await deliverMessage(context, selected.message, { ...options, onExecutionError: policy }, new AbortController().signal, () => true);
        assert.equal(selected.statistics.acknowledged, policy === 'acknowledge' ? 1 : 0);
        assert.equal(selected.statistics.released, policy === 'redeliver' ? 1 : 0);
        assert.deepEqual(result, original);
        assert.deepEqual(context.failures, []);
    }
});

test('acknowledge policy never acknowledges rejected admission, infrastructure, cancellation, crash or unknown results', async () =>
{
    for (const outcome of ['rejected', 'thrown', 'absent', 'error', 'canceled', 'crashed', 'running', 'unknown'])
    {
        const deliveries = [delivery(1), delivery(2)];
        const context = triggerContext({}, {}, (_items, _response, completion) =>
        {
            if (outcome === 'thrown') throw new Error('Emit admission failed');
            if (outcome === 'rejected') completion.reject(new Error('Async admission failed'));
            else completion.resolve(outcome === 'absent' ? undefined : run(outcome));
        });
        await deliverBatch(context, deliveries.map(selected => selected.message), { ...options, onExecutionError: 'acknowledge' }, new AbortController().signal, () => true);
        for (const selected of deliveries)
        {
            assert.equal(selected.statistics.acknowledged, 0);
            assert.equal(selected.statistics.released, 1);
        }
    }
});

test('batch heartbeats cover collection and execution; text, repeated headers, order and item pairing stay intact', async () =>
{
    const text = '{"key":{"id":9223372036854775807},"value":"original\\u0000"}';
    const first = delivery(1, text), second = delivery(2, 'second');
    let continueCollection;
    const gate = new Promise(resolve => { continueCollection = resolve; });
    async function* source()
    {
        yield first.message;
        await gate;
        yield second.message;
    }
    let emitted, completion;
    const context = triggerContext({}, {}, (items, _response, done) => { emitted = items; completion = done; });
    const task = deliverBatch(context, source(), options, new AbortController().signal, () => true);
    await until(() => first.statistics.progress >= 3);
    assert.equal(emitted, undefined);
    assert.equal(first.statistics.acknowledged, 0);
    continueCollection();
    await until(() => completion);
    assert.deepEqual(emitted[0].map(item => item.json.payload), [text, 'second']);
    assert.deepEqual(emitted[0].map(item => item.pairedItem.item), [0, 1]);
    assert.deepEqual(emitted[0].map(item => item.json.metadata.streamSequence), ['1', '2']);
    assert.deepEqual(emitted[0][0].json.metadata.headers['X-Repeated'], ['one', 'two']);
    await until(() => second.statistics.progress >= 3);
    assert.equal(second.statistics.acknowledged, 0);
    completion.resolve(run('success'));
    await task;
    assert.equal(first.statistics.acknowledged, 1);
    assert.equal(second.statistics.acknowledged, 1);
    const settled = [first.statistics.progress, second.statistics.progress];
    await new Promise(resolve => setTimeout(resolve, 20));
    assert.deepEqual([first.statistics.progress, second.statistics.progress], settled);
});

test('malformed batch conversion releases every message without emitting or acknowledging any', async () =>
{
    const deliveries = [delivery(1, '{}'), delivery(2, '{invalid')];
    const context = triggerContext({}, {}, () => assert.fail('Malformed batch must not emit'));
    await deliverBatch(context, deliveries.map(selected => selected.message), { ...options, format: 'json' }, new AbortController().signal, () => true);
    assert.equal(context.failures.length, 1);
    for (const selected of deliveries)
    {
        assert.equal(selected.statistics.acknowledged, 0);
        assert.equal(selected.statistics.released, 1);
    }
});

test('batch ACK ambiguity is recorded without NAKing an uncertain message', async () =>
{
    const first = delivery(1), second = delivery(2);
    first.message.ackAck = async () => { throw new Error('Unknown confirmation'); };
    const context = triggerContext({}, {}, (_items, _response, completion) => completion.resolve(run('success')));
    await deliverBatch(context, [first.message, second.message], options, new AbortController().signal, () => true);
    assert.equal(first.statistics.released, 0);
    assert.equal(second.statistics.released, 0);
    assert.equal(second.statistics.acknowledged, 1);
    assert.equal(context.failures.length, 1);
});

test('unfinished batch shutdown stays recoverable even if the execution later succeeds', async () =>
{
    const deliveries = [delivery(1), delivery(2)];
    const controller = new AbortController();
    let completion;
    const context = triggerContext({}, {}, (_items, _response, done) => { completion = done; });
    const task = deliverBatch(context, deliveries.map(selected => selected.message), { ...options, onExecutionError: 'acknowledge' }, controller.signal, () => true);
    await until(() => completion);
    controller.abort();
    await task;
    completion.resolve(run('success'));
    for (const selected of deliveries)
    {
        assert.equal(selected.statistics.acknowledged, 0);
        assert.equal(selected.statistics.released, 1);
    }
});

test('batch option validation preserves message budgets and defaults without opening a connection', async () =>
{
    for (const selectedOptions of [{ batchSize: 0 }, { batchSize: 1.5 }, { batchSize: 2, maxInFlight: 1 }, { onExecutionError: 'unsupported' }])
    {
        const context = triggerContext({}, { stream: 'TEST', durable: 'test', consumerMode: 'existing', options: selectedOptions }, () => {});
        await assert.rejects(NatsJetstreamTrigger.prototype.trigger.call(context), /Batch|execution error policy/);
    }
    const properties = new NatsJetstreamTrigger().description.properties.find(property => property.name === 'options').options;
    assert.equal(properties.find(property => property.name === 'batchSize').default, 1);
    assert.equal(properties.find(property => property.name === 'onExecutionError').default, 'redeliver');
});
