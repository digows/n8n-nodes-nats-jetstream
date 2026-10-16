const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { mkdtemp, writeFile, rm } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { createServer } = require('node:net');

async function port()
{
    const server = createServer();
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const selected = server.address().port;
    await new Promise(resolve => server.close(resolve));
    return selected;
}

async function until(predicate, milliseconds = 10000)
{
    const deadline = Date.now() + milliseconds;
    while (Date.now() < deadline)
    {
        const value = await predicate();
        if (value) return value;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.fail('Timed out waiting for test condition');
}

async function broker(configuration = '', binary = process.env.NATS_SERVER_BIN)
{
    assert.ok(binary, 'Set NATS_SERVER_BIN to a test nats-server binary');
    const directory = await mkdtemp(join(tmpdir(), 'jetstream-broker-'));
    const serverPort = await port();
    const monitoringPort = await port();
    const configurationPath = join(directory, 'server.conf');
    await writeFile(configurationPath, `host: "127.0.0.1"\nport: ${serverPort}\nhttp_port: ${monitoringPort}\njetstream { store_dir: "${directory}/store" }\n${configuration}`);
    let output = '';
    let processHandle;
    function start()
    {
        output = '';
        processHandle = spawn(binary, ['-c', configurationPath], { stdio: ['ignore', 'pipe', 'pipe'] });
        processHandle.stdout.on('data', chunk => { output += chunk; });
        processHandle.stderr.on('data', chunk => { output += chunk; });
    }
    async function ready()
    {
        await until(() =>
        {
            assert.equal(processHandle.exitCode, null, output);
            return output.includes('Server is ready');
        });
    }
    async function stop()
    {
        if (processHandle.exitCode === null && processHandle.signalCode === null)
        {
            processHandle.kill('SIGTERM');
            await new Promise(resolve => processHandle.once('exit', resolve));
        }
    }
    start();
    try
    {
        await ready();
    }
    catch (error)
    {
        await stop();
        await rm(directory, { recursive: true });
        throw error;
    }
    return {
        directory, serverPort, url: `nats://127.0.0.1:${serverPort}`,
        get processHandle() { return processHandle; },
        get output() { return output; },
        async connections() { return (await (await fetch(`http://127.0.0.1:${monitoringPort}/connz`)).json()).num_connections; },
        async statistics() { return (await (await fetch(`http://127.0.0.1:${monitoringPort}/varz`)).json()); },
        async restart() { await stop(); await start(); await ready(); },
        async close()
        {
            await stop();
            await rm(directory, { recursive: true });
        },
    };
}

function triggerContext(credentials, parameters, emit)
{
    const failures = [], fatal = [];
    return {
        failures, fatal, parameters, logger: { warn() {}, info() {} },
        getMode: () => 'trigger', getNode: () => ({ name: 'JetStream test', type: 'natsJetstreamTrigger', typeVersion: 1 }),
        async getCredentials() { return credentials; },
        getNodeParameter(name) { return parameters[name]; },
        saveFailedExecution(error) { failures.push(error); }, emitError(error) { fatal.push(error); },
        emit,
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

function run(status)
{
    return { status, waitTill: status === 'waiting' ? new Date(Date.now() + 10000) : undefined, data: { resultData: { runData: {} } } };
}

module.exports = { broker, port, until, triggerContext, run };
