const { join } = require('node:path');
const { inspect, isDeepStrictEqual } = require('node:util');

const packageDirectory = join(process.env.N8N_TEST_NODES_DIRECTORY, 'node_modules/n8n-nodes-nats-jetstream');
const { NatsJetstreamTrigger } = require(join(packageDirectory, 'dist/nodes/NatsJetstream/NatsJetstreamTrigger.node.js'));
const { terminalNodeError } = require(join(packageDirectory, 'dist/lib/delivery.js'));
const originalTrigger = NatsJetstreamTrigger.prototype.trigger;

function describe(error)
{
    return error && { prototype: Object.getPrototypeOf(error)?.constructor?.name, ownKeys: Reflect.ownKeys(error).map(String), value: inspect(error, { depth: 3, maxArrayLength: 10, maxStringLength: 1000 }) };
}

NatsJetstreamTrigger.prototype.trigger = function ()
{
    if (this.getNodeParameter('durable') !== 'terminal-ack') return originalTrigger.call(this);
    const context = new Proxy(this, {
        get(target, property)
        {
            if (property !== 'emit')
            {
                const value = Reflect.get(target, property, target);
                return typeof value === 'function' ? value.bind(target) : value;
            }
            return (items, response, completion) =>
            {
                void completion.promise.then(result =>
                {
                    const resultData = result?.data?.resultData;
                    const task = resultData?.runData?.[resultData?.lastNodeExecuted]?.at(-1);
                    console.info(`NATS_TEST_PUBLIC_COMPLETION instrumented fixture, NOT unmodified-artifact proof: ${JSON.stringify({
                        keys: Object.keys(result ?? {}), status: result?.status, jobId: result?.jobId, startedAt: result?.startedAt, stoppedAt: result?.stoppedAt,
                        resultKeys: Object.keys(resultData ?? {}), lastNodeExecuted: resultData?.lastNodeExecuted, runDataKeys: Object.keys(resultData?.runData ?? {}),
                        taskKeys: Object.keys(task ?? {}), executionStatus: task?.executionStatus, topError: describe(resultData?.error), taskError: describe(task?.error),
                        terminalPredicate: terminalNodeError(result), errorsDeepEqual: isDeepStrictEqual(task?.error, resultData?.error),
                    }).slice(0, 12000)}`);
                }, error => console.info(`NATS_TEST_PUBLIC_COMPLETION rejected: ${describe(error)?.value?.slice(0, 2000)}`));
                return target.emit(items, response, completion);
            };
        },
    });
    return originalTrigger.call(context);
};
