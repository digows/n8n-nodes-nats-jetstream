const assert = require('node:assert/strict');
const { test } = require('node:test');
const { Workflow, NodeOperationError } = require('n8n-workflow');
const connectionModule = require('../../dist/lib/connection');
const jetstreamModule = require('@nats-io/jetstream');
const { NatsJetstream } = require('../../dist/nodes/NatsJetstream/NatsJetstream.node');

function capturePublisher(testContext)
{
    const publications = [];
    let closeCount = 0;
    const connection = { async close() { closeCount++; } };
    testContext.mock.method(connectionModule, 'openConnection', async () => connection);
    testContext.mock.getter(jetstreamModule, 'jetstream', () => actualConnection =>
    {
        assert.equal(actualConnection, connection);
        return {
            async publish(subject, data, options)
            {
                publications.push({ subject, data: Buffer.from(data), options });
                return { stream: 'TESTS', seq: publications.length, duplicate: false };
            },
        };
    });
    return { publications, get closeCount() { return closeCount; } };
}

function publisherContext(parameters, inputs = [{ json: {} }], binaryReader)
{
    const publisher = new NatsJetstream();
    const node = {
        id: 'publisher', name: 'Publisher', type: publisher.description.name, typeVersion: 1,
        position: [0, 0], parameters: { subject: 'tests.publish', payloadFormat: 'json', options: {}, ...parameters },
    };
    const workflow = new Workflow({
        nodes: [node], connections: {}, active: false,
        nodeTypes: { getByNameAndVersion: () => publisher },
    });
    return {
        async getCredentials() { return { authentication: 'none', servers: 'nats://test.invalid:4222' }; },
        getNode: () => node, getInputData: () => inputs, continueOnFail: () => false,
        getNodeParameter(name, itemIndex, fallbackValue, options)
        {
            const rawValue = Object.hasOwn(node.parameters, name) ? node.parameters[name] : fallbackValue;
            if (options?.rawExpressions) return rawValue;
            // Evaluate with the public n8n evaluator, not a hand-built expression stub.
            return workflow.expression.getParameterValue(rawValue, null, 0, itemIndex, node.name, inputs, 'manual', {});
        },
        helpers: { getBinaryDataBuffer: binaryReader },
    };
}

test('execute preserves expression strings, including JSON-looking strings, per item', async testContext =>
{
    const captured = capturePublisher(testContext);
    const values = ['hello', '123', 'false', 'null', '{"value":1}', '[1,2]', '"quoted"', '=not-an-expression', ''];
    const context = publisherContext({ jsonPayload: '={{ $json.value }}' }, values.map(value => ({ json: { value } })));
    const [results] = await NatsJetstream.prototype.execute.call(context);
    assert.equal(captured.publications.length, values.length);
    captured.publications.forEach((publication, itemIndex) =>
    {
        assert.equal(publication.data.toString('utf8'), JSON.stringify(values[itemIndex]));
        assert.deepEqual(results[itemIndex].pairedItem, { item: itemIndex });
    });
    assert.equal(captured.closeCount, 1);
});

test('execute preserves native expression number, boolean, null, object and array values', async testContext =>
{
    const captured = capturePublisher(testContext);
    const values = [123, 0, 1.25, true, false, null, { value: '123', nested: { enabled: false } }, [1, 'false', null]];
    const context = publisherContext({ jsonPayload: '={{ $json.value }}' }, values.map(value => ({ json: { value } })));
    await NatsJetstream.prototype.execute.call(context);
    assert.deepEqual(captured.publications.map(publication => publication.data.toString('utf8')), values.map(value => JSON.stringify(value)));
    assert.equal(captured.closeCount, 1);
});

test('execute preserves direct expression literal strings', async testContext =>
{
    const captured = capturePublisher(testContext);
    for (const value of ['123', 'false', 'hello'])
    {
        const context = publisherContext({ jsonPayload: `={{ ${JSON.stringify(value)} }}` });
        await NatsJetstream.prototype.execute.call(context);
    }
    assert.deepEqual(captured.publications.map(publication => publication.data.toString('utf8')), ['"123"', '"false"', '"hello"']);
    assert.equal(captured.closeCount, 3);
});

test('execute parses fixed JSON editor text once before serializing', async testContext =>
{
    const captured = capturePublisher(testContext);
    const fixedValues = [' { "value": 1 } ', '[1,"false",null]', '123', 'true', 'false', 'null', '"hello"', '"123"', '"false"'];
    for (const fixedValue of fixedValues)
    {
        await NatsJetstream.prototype.execute.call(publisherContext({ jsonPayload: fixedValue }));
    }
    assert.deepEqual(captured.publications.map(publication => publication.data.toString('utf8')),
        fixedValues.map(value => JSON.stringify(JSON.parse(value))));
    assert.equal(captured.closeCount, fixedValues.length);
});

test('execute rejects invalid fixed JSON before publishing and closes the connection', async testContext =>
{
    const captured = capturePublisher(testContext);
    for (const fixedValue of ['hello', '{invalid', ''])
    {
        await assert.rejects(NatsJetstream.prototype.execute.call(publisherContext({ jsonPayload: fixedValue })),
            error => error instanceof NodeOperationError);
    }
    assert.equal(captured.publications.length, 0);
    assert.equal(captured.closeCount, 3);
});

test('execute keeps text encoding and binary helper behavior unchanged', async testContext =>
{
    const captured = capturePublisher(testContext);
    await NatsJetstream.prototype.execute.call(publisherContext(
        { payloadFormat: 'text', textPayload: '={{ $json.value }}' }, [{ json: { value: 'Olá "123"' } }]));
    const binary = Buffer.from([0, 1, 255]);
    let binaryReads = 0;
    await NatsJetstream.prototype.execute.call(publisherContext(
        { payloadFormat: 'binary', binaryProperty: 'data' }, [{ json: {} }], async (itemIndex, field) =>
        {
            assert.equal(itemIndex, 0);
            assert.equal(field, 'data');
            binaryReads++;
            return binary;
        }));
    assert.deepEqual(captured.publications.map(publication => publication.data), [Buffer.from('Olá "123"', 'utf8'), binary]);
    assert.equal(binaryReads, 1);
    assert.equal(captured.closeCount, 2);
});
