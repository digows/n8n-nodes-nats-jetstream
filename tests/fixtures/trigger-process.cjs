const { triggerContext, run } = require('../helpers.cjs');
const { JetstreamNatsTrigger } = require('../../dist/nodes/JetstreamNats/JetstreamNatsTrigger.node');

const completions = new Map();
let response;
process.on('message', async command =>
{
    if (command.type === 'complete') completions.get(command.sequence)?.resolve(run('success'));
    if (command.type === 'close')
    {
        await response.closeFunction();
        process.disconnect();
    }
});
const context = triggerContext({ servers: process.env.TEST_NATS_URL, authentication: 'none' }, {
    stream: 'LIFECYCLE', durable: 'shared', consumerMode: 'existing', payloadFormat: 'json',
    options: { maxInFlight: 1, pullTimeoutMs: 1000, shutdownGraceMs: 100, nakDelayMs: 100 },
}, (items, _unused, completion) =>
{
    const metadata = items[0][0].json.metadata;
    completions.set(metadata.streamSequence, completion);
    process.send({ type: 'delivery', metadata });
});
context.emitError = () => process.send({ type: 'fatal' });
JetstreamNatsTrigger.prototype.trigger.call(context).then(result =>
{
    response = result;
    process.send({ type: 'ready' });
}).catch(error => { process.send({ type: 'error', message: error.message }); process.exitCode = 1; process.disconnect(); });
