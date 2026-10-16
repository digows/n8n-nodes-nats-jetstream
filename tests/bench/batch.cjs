const assert = require('node:assert/strict');
const { performance } = require('node:perf_hooks');
const { connect } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, StorageType } = require('@nats-io/jetstream');
const { broker, until, triggerContext, run } = require('../helpers.cjs');
const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node');

async function main()
{
    const messageCount = 1000;
    const completionDelayMs = 2;
    const server = await broker();
    let connection;
    const results = [];
    try
    {
        connection = await connect({ servers: server.url });
        const manager = await jetstreamManager(connection), client = jetstream(connection);
        await manager.streams.add({ name: 'BENCHMARK', subjects: ['benchmark.>'], storage: StorageType.File });
        for (const scenario of [
            { name: 'single-serial', batchSize: 1, maxInFlight: 1 },
            { name: 'single-concurrent', batchSize: 1, maxInFlight: 50 },
            { name: 'batch-serial', batchSize: 50, maxInFlight: 50 },
        ])
        {
            for (let index = 0; index < messageCount; index++) await client.publish(`benchmark.${scenario.name}`, Buffer.from(JSON.stringify({ index })));
            const completed = new Set(), latencies = [], tasks = new Set();
            let executionCount = 0, response;
            const started = performance.now();
            const context = triggerContext({ servers: server.url, authentication: 'none' }, {
                stream: 'BENCHMARK', durable: scenario.name, consumerMode: 'create', payloadFormat: 'json',
                creation: { subjectFilters: `benchmark.${scenario.name}`, deliverPolicy: 'all', ackWaitMs: 1000, maxAckPending: -1 },
                options: { batchSize: scenario.batchSize, maxInFlight: scenario.maxInFlight, pullTimeoutMs: 1000, shutdownGraceMs: 1000 },
            }, (outputs, _response, completion) =>
            {
                executionCount++;
                const task = new Promise(resolve => setTimeout(resolve, completionDelayMs)).then(() =>
                {
                    const latency = performance.now() - started;
                    for (const item of outputs[0])
                    {
                        const index = item.json.payload.index;
                        assert.equal(completed.has(index), false, 'Benchmark must not hide duplicate delivery');
                        completed.add(index);
                        latencies.push(latency);
                    }
                    completion.resolve(run('success'));
                });
                tasks.add(task);
                void task.finally(() => tasks.delete(task));
            });
            try
            {
                response = await NatsJetstreamTrigger.prototype.trigger.call(context);
                await until(async () => completed.size === messageCount && (await manager.consumers.info('BENCHMARK', scenario.name)).num_ack_pending === 0, 60000);
                const elapsedMs = performance.now() - started;
                assert.deepEqual(context.failures, []);
                assert.deepEqual(context.fatal, []);
                assert.equal(executionCount, messageCount / scenario.batchSize);
                assert.equal((await manager.consumers.info('BENCHMARK', scenario.name)).num_redelivered, 0);
                latencies.sort((left, right) => left - right);
                const percentile = value => Number(latencies[Math.ceil(value * latencies.length) - 1].toFixed(2));
                results.push({
                    scenario: scenario.name, batchSize: scenario.batchSize, maxInFlightMessages: scenario.maxInFlight,
                    messages: completed.size, executions: executionCount, elapsedThroughConfirmedAcksMs: Number(elapsedMs.toFixed(2)),
                    messagesPerSecond: Number((messageCount * 1000 / elapsedMs).toFixed(2)),
                    completionLatencyMs: { p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99) },
                });
            }
            finally
            {
                await response?.closeFunction();
                await Promise.all(tasks);
            }
        }
        process.stdout.write(`${JSON.stringify({
            measurement: 'Synthetic public-interface completion adapter, NOT an n8n host throughput measurement',
            broker: connection.info.version, sdk: require('../../package-lock.json').packages['node_modules/@nats-io/jetstream'].version, node: process.version,
            completionDelayMs, workload: 'Prepublished backlog; clock includes trigger activation, execution adapter and confirmed ACKs; excludes publication',
            limitations: 'Fixed per-execution cost favors batching; no application I/O, n8n admission/persistence or low-volume partial-batch timeout in this measurement',
            results,
        }, null, 2)}\n`);
    }
    finally
    {
        await connection?.close();
        await server.close();
    }
}

main().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; });
