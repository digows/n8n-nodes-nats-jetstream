const assert = require('node:assert/strict');
const { test } = require('node:test');
const { spawn } = require('node:child_process');
const { mkdtemp, mkdir, symlink, rm } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { pathToFileURL } = require('node:url');
const { DatabaseSync } = require('node:sqlite');
const { createServer } = require('node:http');
const { parse } = require('flatted');
const { connect } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager, StorageType } = require('@nats-io/jetstream');
const { broker, port, until } = require('../helpers.cjs');

async function stop(child)
{
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    child.kill('SIGTERM');
    let timer;
    await Promise.race([
        new Promise(resolve => child.once('exit', resolve)),
        new Promise(resolve => { timer = setTimeout(() => { child.kill('SIGKILL'); resolve(); }, 10000); }),
    ]);
    clearTimeout(timer);
}

async function host(directory, mode, postgres, redisPort)
{
    assert.ok(process.env.N8N_TEST_CLI, 'Set N8N_TEST_CLI to the target n8n bin/n8n');
    const serverPort = await port();
    const brokerPort = await port();
    const environment = {
        ...process.env, N8N_USER_FOLDER: directory, N8N_PORT: String(serverPort), N8N_LISTEN_ADDRESS: '127.0.0.1',
        N8N_DIAGNOSTICS_ENABLED: 'false', N8N_VERSION_NOTIFICATIONS_ENABLED: 'false', N8N_LOG_LEVEL: 'warn',
        N8N_ENCRYPTION_KEY: 'synthetic-test-only-encryption-key', N8N_SECURE_COOKIE: 'false',
        N8N_RUNNERS_BROKER_PORT: String(brokerPort), EXECUTIONS_MODE: mode, OFFLOAD_MANUAL_EXECUTIONS_TO_WORKERS: 'true',
        DB_TYPE: mode === 'queue' ? 'postgresdb' : 'sqlite',
        ...(mode === 'queue' ? { DB_POSTGRESDB_HOST: '127.0.0.1', DB_POSTGRESDB_PORT: String(postgres.port), DB_POSTGRESDB_DATABASE: 'n8n_test', DB_POSTGRESDB_USER: 'postgres', DB_POSTGRESDB_PASSWORD: 'test-password', QUEUE_BULL_REDIS_HOST: '127.0.0.1', QUEUE_BULL_REDIS_PORT: String(redisPort) } : {}),
    };
    await mkdir(join(directory, '.n8n'), { recursive: true });
    await symlink(process.env.N8N_TEST_NODES_DIRECTORY, join(directory, '.n8n', 'nodes'));
    let child, worker, output = '', cookie = '';
    const url = `http://127.0.0.1:${serverPort}`;
    async function request(path, method = 'GET', body)
    {
        const response = await fetch(url + path, { method, headers: { 'Content-Type': 'application/json', Cookie: cookie }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
        if (response.headers.get('set-cookie')) cookie = response.headers.get('set-cookie').split(';')[0];
        const responseBytes = await response.arrayBuffer();
        const responseText = new TextDecoder().decode(responseBytes);
        let result;
        try { result = JSON.parse(responseText); }
        catch (error)
        {
            throw new Error(`${method} ${path} HTTP ${response.status}: invalid JSON, ${responseBytes.byteLength} bytes, tail=${JSON.stringify(responseText.slice(-256))}`, { cause: error });
        }
        assert.ok(response.ok, `${method} ${path} ${response.status}: ${JSON.stringify(result)}`);
        return result.data ?? result;
    }
    async function start()
    {
        child = spawn(process.execPath, ['--require', join(__dirname, '../fixtures/completion-diagnostic.cjs'), process.env.N8N_TEST_CLI, 'start'], { env: environment, stdio: ['ignore', 'pipe', 'pipe'] });
        child.stdout.on('data', chunk => { output += chunk; }); child.stderr.on('data', chunk => { output += chunk; });
        await until(async () =>
        {
            assert.equal(child.exitCode, null, output);
            try { return (await fetch(url + '/healthz/readiness')).ok; } catch { return false; }
        }, 60000);
    }
    try
    {
        await start();
        await request('/rest/owner/setup', 'POST', { email: 'tester@example.invalid', firstName: 'Test', lastName: 'Owner', password: 'SyntheticPassw0rd!123' });
    }
    catch (error) { await stop(child); error.stack += `\n${output}`; throw error; }
    async function startWorker()
    {
        worker = spawn(process.execPath, ['--require', join(__dirname, '../fixtures/completion-diagnostic.cjs'), process.env.N8N_TEST_CLI, 'worker', '--concurrency=2'], { env: { ...environment, N8N_RUNNERS_BROKER_PORT: String(await port()) }, stdio: ['ignore', 'pipe', 'pipe'] });
        worker.stdout.on('data', chunk => { output += chunk; }); worker.stderr.on('data', chunk => { output += chunk; });
    }
    return {
        request, url, startWorker, get output() { return output; },
        async restart()
        {
            await stop(child); await start();
            await request('/rest/login', 'POST', { emailOrLdapLoginId: 'tester@example.invalid', password: 'SyntheticPassw0rd!123' });
        },
        async close() { await stop(worker); await stop(child); },
    };
}

for (const mode of ['regular', 'queue'])
{
    test(`n8n ${process.env.N8N_TEST_HOST_VERSION ?? '2.42.3'} ${mode}: persisted Wait, errors, retries, batches and manual execution`, { timeout: 240000 }, async suite =>
    {
        const directory = await mkdtemp(join(tmpdir(), `jetstream-host-${mode}-`));
        const server = await broker();
        const connection = await connect({ servers: server.url });
        const manager = await jetstreamManager(connection);
        const client = jetstream(connection);
        let postgres, databaseClient, redis, instance;
        let sqlite;
        let slowServer;
        const requests = [], responseTimers = new Set();
        try
        {
            await manager.streams.add({ name: 'HOST', subjects: ['host.>'], storage: StorageType.File });
            if (mode === 'queue')
            {
                assert.ok(process.env.EMBEDDED_POSTGRES_MODULE, 'Set EMBEDDED_POSTGRES_MODULE for queue-mode tests');
                const { default: EmbeddedPostgres } = await import(pathToFileURL(process.env.EMBEDDED_POSTGRES_MODULE));
                const postgresPort = await port();
                const cluster = new EmbeddedPostgres({ databaseDir: join(directory, 'postgres'), port: postgresPort, user: 'postgres', password: 'test-password', persistent: true, createPostgresUser: false, onLog() {}, onError() {} });
                await cluster.initialise(); await cluster.start(); await cluster.createDatabase('n8n_test');
                postgres = { port: postgresPort, cluster };
                databaseClient = cluster.getPgClient('n8n_test'); await databaseClient.connect();
                const redisPort = await port();
                redis = spawn(process.env.REDIS_SERVER_BIN ?? 'redis-server', ['--bind', '127.0.0.1', '--port', String(redisPort), '--save', '', '--appendonly', 'no', '--dir', directory], { stdio: 'ignore' });
                instance = await host(join(directory, 'host'), mode, postgres, redisPort);
            }
            else
            {
                instance = await host(join(directory, 'host'), mode);
                sqlite = new DatabaseSync(join(directory, 'host', '.n8n', 'database.sqlite'));
                sqlite.exec('PRAGMA busy_timeout = 5000');
            }
            const nodeDescriptions = await instance.request('/types/nodes.json');
            const descriptions = Array.isArray(nodeDescriptions) ? nodeDescriptions : Object.values(nodeDescriptions);
            assert.ok(descriptions.some(value => value.name === 'n8n-nodes-nats-jetstream.natsJetstreamTrigger'), 'Installed trigger must appear');
            assert.ok(descriptions.some(value => value.name === 'n8n-nodes-nats-jetstream.natsJetstream'), 'Installed publisher must appear');
            const credential = await instance.request('/rest/credentials', 'POST', { name: 'Local NATS Test', type: 'natsJetstreamApi', data: { servers: server.url, authentication: 'none' } });
            async function createWorkflow(name, suffix, failAfterWait = false, activate = true)
            {
                const trigger = { id: `trigger-${suffix}`, name: 'JetStream', type: 'n8n-nodes-nats-jetstream.natsJetstreamTrigger', typeVersion: 1, position: [0, 0], credentials: { natsJetstreamApi: { id: credential.id, name: credential.name } }, parameters: { stream: 'HOST', durable: suffix, consumerMode: 'create', creation: { subjectFilters: `host.${suffix}`, ackWaitMs: 150 }, payloadFormat: 'json', options: { maxInFlight: 2, nakDelayMs: 100, pullTimeoutMs: 1000, shutdownGraceMs: 100 } } };
                const wait = { id: `wait-${suffix}`, webhookId: `wait-${suffix}`, name: 'Wait', type: 'n8n-nodes-base.wait', typeVersion: 1.1, position: [200, 0], parameters: { resume: 'webhook', httpMethod: 'POST', responseMode: 'onReceived', options: {} } };
                const after = { id: `after-${suffix}`, name: 'After', type: failAfterWait ? 'n8n-nodes-base.stopAndError' : 'n8n-nodes-base.noOp', typeVersion: 1, position: [400, 0], parameters: failAfterWait ? { errorMessage: 'Synthetic failure after Wait' } : {} };
                const workflow = await instance.request('/rest/workflows', 'POST', { name, nodes: [trigger, wait, after], connections: { JetStream: { main: [[{ node: 'Wait', type: 'main', index: 0 }]] }, Wait: { main: [[{ node: 'After', type: 'main', index: 0 }]] } }, settings: { executionOrder: 'v1', saveDataSuccessExecution: 'all', saveDataErrorExecution: 'all', saveManualExecutions: true } });
                if (!activate) return workflow;
                await instance.request(`/rest/workflows/${workflow.id}/activate`, 'POST', { versionId: workflow.versionId });
                await until(async () =>
                {
                    try { return await manager.consumers.info('HOST', suffix); }
                    catch (error) { if (error.code === 10014) return false; throw error; }
                }, 15000);
                return workflow;
            }
            const waitingWorkflow = await createWorkflow('Wait test', 'wait', true);
            if (mode === 'queue')
            {
                for (let index = 0; index < 3; index++) await client.publish('host.wait', Buffer.from('{}'));
                await until(async () => (await manager.consumers.info('HOST', 'wait')).num_ack_pending === 2);
                await new Promise(resolve => setTimeout(resolve, 600));
                const backlog = await manager.consumers.info('HOST', 'wait');
                assert.equal(backlog.num_ack_pending, 2); assert.equal(backlog.num_redelivered, 0); assert.equal(backlog.num_pending, 1);
                await instance.startWorker();
            }
            else await client.publish('host.wait', Buffer.from('{}'));
            async function executions(workflowId)
            {
                if (sqlite) return sqlite.prepare('SELECT id, status, waitTill FROM execution_entity WHERE workflowId = ? ORDER BY id').all(workflowId);
                return (await databaseClient.query('SELECT id, status, "waitTill" FROM execution_entity WHERE "workflowId" = $1 ORDER BY id', [workflowId])).rows;
            }
            await until(async () =>
            {
                const consumerInfo = await manager.consumers.info('HOST', 'wait');
                const rows = await executions(waitingWorkflow.id);
                return consumerInfo.num_ack_pending === 0 && consumerInfo.num_pending === 0 && rows.length === (mode === 'queue' ? 3 : 1);
            }, 30000);
            const waitingRows = await executions(waitingWorkflow.id);
            assert.equal(waitingRows.length, mode === 'queue' ? 3 : 1);
            assert.ok(waitingRows.every(row => row.status === 'waiting' && row.waitTill), 'ACK must correspond to durable waiting rows');
            await instance.restart();
            for (const row of waitingRows)
            {
                const execution = await instance.request(`/rest/executions/${row.id}`);
                const executionData = typeof execution.data === 'string' ? parse(execution.data) : execution.data;
                assert.ok(executionData.resumeToken, 'Persisted execution must contain a resume token');
                const response = await fetch(instance.url + `/webhook-waiting/${row.id}?signature=${executionData.resumeToken}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
                assert.ok(response.ok, `Wait resume returned ${response.status}`);
            }
            await until(async () => (await executions(waitingWorkflow.id)).every(row => row.status === 'error'), 30000);
            assert.equal((await manager.consumers.info('HOST', 'wait')).num_redelivered, 0);
            await instance.request(`/rest/workflows/${waitingWorkflow.id}/deactivate`, 'POST', {});
            suite.diagnostic('Native Wait persistence, restart/resume and post-handoff failure assertions passed');
            const failingWorkflow = await createWorkflow('Wait persistence failure', 'persist');
            if (sqlite)
            {
                sqlite.exec(`CREATE TRIGGER fail_wait BEFORE UPDATE ON execution_entity WHEN NEW.status = 'waiting' AND NEW.workflowId = '${failingWorkflow.id}' BEGIN SELECT RAISE(ABORT, 'Synthetic Wait persistence failure'); END;`);
            }
            else
            {
                await databaseClient.query(`CREATE FUNCTION fail_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'waiting' AND NEW."workflowId" = '${failingWorkflow.id}' THEN RAISE EXCEPTION 'Synthetic Wait persistence failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_wait BEFORE UPDATE ON execution_entity FOR EACH ROW EXECUTE FUNCTION fail_wait();`);
            }
            await client.publish('host.persist', Buffer.from('{}'));
            await until(async () => (await manager.consumers.info('HOST', 'persist')).num_redelivered > 0, 30000);
            const failingRows = await executions(failingWorkflow.id);
            assert.ok(failingRows.length > 0);
            assert.ok(failingRows.every(row => row.status !== 'waiting'), 'Persistence failure must not create a successful handoff');
            await instance.request(`/rest/workflows/${failingWorkflow.id}/deactivate`, 'POST', {});
            suite.diagnostic('Native Wait persistence rejection and broker redelivery assertions passed');
            const manualWorkflow = await createWorkflow('Real repeated manual trigger', 'manual', false, false);
            for (let index = 0; index < 2; index++)
            {
                await instance.request(`/rest/workflows/${manualWorkflow.id}/run`, 'POST', { triggerToStartFrom: { name: 'JetStream' } });
                await until(async () =>
                {
                    try { return await manager.consumers.info('HOST', 'manual'); }
                    catch (error) { if (error.code === 10014) return false; throw error; }
                });
                await client.publish('host.manual', Buffer.from(JSON.stringify({ manualTest: index })));
                await until(async () =>
                {
                    const rows = await executions(manualWorkflow.id);
                    return rows.length === index + 1 && rows.every(row => row.status === 'waiting') && (await manager.consumers.info('HOST', 'manual')).num_ack_pending === 0;
                }, 30000);
            }
            const manualRows = await executions(manualWorkflow.id);
            for (const row of manualRows)
            {
                const execution = await instance.request(`/rest/executions/${row.id}`);
                assert.equal(execution.mode, 'manual');
                const data = typeof execution.data === 'string' ? parse(execution.data) : execution.data;
                const resume = await fetch(instance.url + `/webhook-waiting/${row.id}?signature=${data.resumeToken}`, { method: 'POST' });
                assert.ok(resume.ok);
            }
            await until(async () => (await executions(manualWorkflow.id)).every(row => row.status === 'success'), 30000);
            await client.publish('host.manual', Buffer.from('{}'));
            await new Promise(resolve => setTimeout(resolve, 400));
            assert.equal((await manager.consumers.info('HOST', 'manual')).num_pending, 1, 'Manual listener must stop after its execution');
            assert.equal((await executions(manualWorkflow.id)).length, 2);
            await until(async () => await server.connections() === 1);
            slowServer = createServer((request, response) =>
            {
                const url = new URL(request.url, 'http://localhost');
                requests.push(url.pathname);
                if (url.pathname === '/native-retry' && requests.filter(path => path === '/native-retry').length < 3) response.statusCode = 503;
                if (url.pathname === '/terminal-http') response.statusCode = 401;
                const timer = setTimeout(() => { responseTimers.delete(timer); response.setHeader('Content-Type', 'application/json'); response.end('{}'); }, Number(url.searchParams.get('delayMs')));
                responseTimers.add(timer);
                response.on('close', () => { clearTimeout(timer); responseTimers.delete(timer); });
            });
            await new Promise(resolve => slowServer.listen(0, '127.0.0.1', resolve));
            async function createManualWorkflow(suffix, graceMs, delayMs = 0, persistedWait = false)
            {
                const trigger = { id: `manual-${suffix}`, name: 'JetStream', type: 'n8n-nodes-nats-jetstream.natsJetstreamTrigger', typeVersion: 1, position: [0, 0], credentials: { natsJetstreamApi: { id: credential.id, name: credential.name } }, parameters: { stream: 'HOST', durable: suffix, consumerMode: 'create', creation: { subjectFilters: `host.${suffix}`, ackWaitMs: 150 }, payloadFormat: 'json', options: { nakDelayMs: 100, pullTimeoutMs: 1000, shutdownGraceMs: graceMs } } };
                const after = persistedWait
                    ? { id: `wait-${suffix}`, webhookId: `wait-${suffix}`, name: 'After', type: 'n8n-nodes-base.wait', typeVersion: 1.1, position: [200, 0], parameters: { resume: 'webhook', httpMethod: 'POST', responseMode: 'onReceived', options: {} } }
                    : delayMs > 0
                        ? { id: `http-${suffix}`, name: 'After', type: 'n8n-nodes-base.httpRequest', typeVersion: 4.2, position: [200, 0], parameters: { url: `http://127.0.0.1:${slowServer.address().port}/${suffix}?delayMs=${delayMs}`, options: {} } }
                        : { id: `after-${suffix}`, name: 'After', type: 'n8n-nodes-base.noOp', typeVersion: 1, position: [200, 0], parameters: {} };
                return await instance.request('/rest/workflows', 'POST', { name: suffix, nodes: [trigger, after], connections: { JetStream: { main: [[{ node: 'After', type: 'main', index: 0 }]] } }, settings: { executionOrder: 'v1', saveDataSuccessExecution: 'all', saveDataErrorExecution: 'all', saveManualExecutions: true } });
            }
            async function beginManual(workflow, suffix)
            {
                const result = await instance.request(`/rest/workflows/${workflow.id}/run`, 'POST', { triggerToStartFrom: { name: 'JetStream' } });
                assert.ok(result.executionId, 'Manual REST execution must return an execution ID');
                await until(async () =>
                {
                    try { return (await server.connections()) === 2 && await manager.consumers.info('HOST', suffix); }
                    catch (error) { if (error.code === 10014) return false; throw error; }
                }, 15000);
                return result.executionId;
            }
            async function assertCleanup()
            {
                await until(async () => await server.connections() === 1, 5000);
                const before = await server.statistics();
                await new Promise(resolve => setTimeout(resolve, 300));
                assert.equal((await server.statistics()).in_msgs, before.in_msgs, 'Settled manual trigger must not continue sending progress/pulls');
            }
            async function assertRecovery(suffix)
            {
                const consumer = await client.consumers.get('HOST', suffix);
                const message = await consumer.next({ expires: 1000 });
                assert.ok(message, 'Unacknowledged manual message must be recoverable');
                assert.equal(message.redelivered, true);
                assert.equal(await message.ackAck(), true);
            }
            const successfulManual = await createManualWorkflow('manual-success', 2000, 500);
            await beginManual(successfulManual, 'manual-success');
            await client.publish('host.manual-success', Buffer.from('{}'));
            await until(() => requests.includes('/manual-success'));
            await new Promise(resolve => setTimeout(resolve, 250));
            assert.equal((await manager.consumers.info('HOST', 'manual-success')).num_ack_pending, 1, 'Manual close must not ACK while HTTP processing is incomplete');
            await until(async () => (await executions(successfulManual.id)).every(row => row.status === 'success') && (await manager.consumers.info('HOST', 'manual-success')).num_ack_pending === 0);
            assert.equal((await manager.consumers.info('HOST', 'manual-success')).num_redelivered, 0);
            await assertCleanup();
            suite.diagnostic('Real manual success beyond AckWait and repeated persisted Wait/resume passed');
            const cancelBefore = await createManualWorkflow('manual-cancel-before', 2000);
            const cancelBeforeId = await beginManual(cancelBefore, 'manual-cancel-before');
            await instance.request(`/rest/executions/${cancelBeforeId}/stop`, 'POST', {});
            await until(async () => (await executions(cancelBefore.id))[0]?.status === 'canceled');
            await assertCleanup();
            assert.equal((await manager.consumers.info('HOST', 'manual-cancel-before')).delivered.consumer_seq, 0);
            const cancelAfter = await createManualWorkflow('manual-cancel-after', 2000, 5000);
            const cancelAfterId = await beginManual(cancelAfter, 'manual-cancel-after');
            await client.publish('host.manual-cancel-after', Buffer.from('{}'));
            await until(() => requests.includes('/manual-cancel-after'));
            await instance.request(`/rest/executions/${cancelAfterId}/stop`, 'POST', {});
            await until(async () => (await executions(cancelAfter.id))[0]?.status === 'canceled');
            await assertCleanup(); await assertRecovery('manual-cancel-after');
            suite.diagnostic('Real manual cancellation before message and during execution released connections/messages');
            const expiredManual = await createManualWorkflow('manual-expired', 250, 1500);
            await beginManual(expiredManual, 'manual-expired');
            const deadlineStart = performance.now();
            await client.publish('host.manual-expired', Buffer.from('{}'));
            await until(() => requests.includes('/manual-expired'));
            await assertCleanup();
            const cleanupElapsed = performance.now() - deadlineStart;
            assert.ok(cleanupElapsed < 1300, `Manual cleanup took ${cleanupElapsed} ms with a 250 ms grace`);
            await assertRecovery('manual-expired');
            await until(async () => (await executions(expiredManual.id))[0]?.status === 'success');
            assert.equal((await manager.consumers.info('HOST', 'manual-expired')).num_ack_pending, 0);
            suite.diagnostic(`Manual 250 ms grace released unfinished HTTP work before its 1500 ms success; measured publish-to-cleanup ${Math.round(cleanupElapsed)} ms including 300 ms inactivity assertion`);
            const manualPersistenceFailure = await createManualWorkflow('manual-persist-failure', 1000, 0, true);
            if (sqlite) sqlite.exec(`CREATE TRIGGER fail_manual_wait BEFORE UPDATE ON execution_entity WHEN NEW.status = 'waiting' AND NEW.workflowId = '${manualPersistenceFailure.id}' BEGIN SELECT RAISE(ABORT, 'Synthetic manual Wait persistence failure'); END;`);
            else await databaseClient.query(`CREATE FUNCTION fail_manual_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'waiting' AND NEW."workflowId" = '${manualPersistenceFailure.id}' THEN RAISE EXCEPTION 'Synthetic manual Wait persistence failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_manual_wait BEFORE UPDATE ON execution_entity FOR EACH ROW EXECUTE FUNCTION fail_manual_wait();`);
            await beginManual(manualPersistenceFailure, 'manual-persist-failure');
            await client.publish('host.manual-persist-failure', Buffer.from('{}'));
            await assertCleanup();
            assert.ok((await executions(manualPersistenceFailure.id)).every(row => row.status !== 'waiting'));
            await assertRecovery('manual-persist-failure');
            suite.diagnostic('Real manual Wait persistence failure caused no success ACK and remained recoverable');

            async function createProcessingWorkflow(suffix, processingNodes, selectedOptions = {}, format = 'json', activate = true)
            {
                const trigger = { id: `processing-${suffix}`, name: 'JetStream', type: 'n8n-nodes-nats-jetstream.natsJetstreamTrigger', typeVersion: 1, position: [0, 0], credentials: { natsJetstreamApi: { id: credential.id, name: credential.name } }, parameters: { stream: 'HOST', durable: suffix, consumerMode: 'create', creation: { subjectFilters: `host.${suffix}`, ackWaitMs: 150, maxDeliver: 2, maxAckPending: -1 }, payloadFormat: format, options: { maxInFlight: 2, nakDelayMs: 100, pullTimeoutMs: 1000, shutdownGraceMs: 2000, ...selectedOptions } } };
                const nodes = [trigger, ...processingNodes];
                const connections = Object.fromEntries(nodes.slice(0, -1).map((node, index) => [node.name, { main: [[{ node: nodes[index + 1].name, type: 'main', index: 0 }]] }]));
                const workflow = await instance.request('/rest/workflows', 'POST', { name: suffix, nodes, connections, settings: { executionOrder: 'v1', saveDataSuccessExecution: 'all', saveDataErrorExecution: 'all', saveManualExecutions: true } });
                if (activate)
                {
                    await instance.request(`/rest/workflows/${workflow.id}/activate`, 'POST', { versionId: workflow.versionId });
                    await until(async () =>
                    {
                        try { return await manager.consumers.info('HOST', suffix); }
                        catch (error) { if (error.code === 10014) return false; throw error; }
                    }, 15000);
                }
                return workflow;
            }
            const terminalNode = { id: 'terminal-failure', name: 'Fail', type: 'n8n-nodes-base.stopAndError', typeVersion: 1, position: [400, 0], parameters: { errorMessage: 'Synthetic terminal node failure' } };
            const collectNode = { id: 'collect-batch', name: 'Collect', type: 'n8n-nodes-base.code', typeVersion: 2, position: [200, 0], parameters: { jsCode: 'return $input.all();' } };
            const terminal = await createProcessingWorkflow('terminal-ack', [collectNode, terminalNode], { batchSize: 2, onExecutionError: 'acknowledge' }, 'text');
            const rawPayloads = ['{"id":9223372036854775807,"value":"original\\u0000"}', '{"id":9223372036854775806}'];
            for (const payload of rawPayloads) await client.publish('host.terminal-ack', Buffer.from(payload));
            try
            {
                await until(async () =>
                {
                    const rows = await executions(terminal.id);
                    return rows.length === 1 && rows[0].status === 'error' && (await manager.consumers.info('HOST', 'terminal-ack')).num_ack_pending === 0;
                }, 30000);
            }
            catch (error)
            {
                try
                {
                    const rows = await executions(terminal.id);
                    const results = [];
                    for (const row of rows.slice(-3))
                    {
                        const execution = await instance.request(`/rest/executions/${row.id}`);
                        const data = typeof execution.data === 'string' ? parse(execution.data) : execution.data;
                        const resultData = data?.resultData;
                        results.push({ id: row.id, status: execution.status, stoppedAt: execution.stoppedAt, finished: execution.finished,
                            lastNodeExecuted: resultData?.lastNodeExecuted, error: resultData?.error,
                            tasks: Object.fromEntries(Object.entries(resultData?.runData ?? {}).map(([name, tasks]) => [name, tasks.slice(-2).map(task => ({ executionStatus: task.executionStatus, error: task.error }))])),
                        });
                    }
                    suite.diagnostic(`Terminal-error timeout (persisted execution view, not a captured completion callback): ${JSON.stringify({ mode, rows, consumer: await manager.consumers.info('HOST', 'terminal-ack'), results }).slice(0, 16000)}`);
                }
                catch (diagnosticError) { suite.diagnostic(`Terminal-error diagnostic failed: ${diagnosticError.message}`); }
                throw error;
            }
            const terminalRow = (await executions(terminal.id))[0];
            const terminalExecution = await instance.request(`/rest/executions/${terminalRow.id}`);
            const terminalData = typeof terminalExecution.data === 'string' ? parse(terminalExecution.data) : terminalExecution.data;
            assert.equal(terminalData.resultData.error.message, 'Synthetic terminal node failure');
            const collected = terminalData.resultData.runData.Collect[0].data.main[0];
            assert.deepEqual(collected.map(item => item.json.payload), rawPayloads);
            assert.deepEqual(collected.map(item => item.pairedItem.item), [0, 1]);
            assert.equal((await manager.consumers.info('HOST', 'terminal-ack')).num_redelivered, 0);
            await instance.request(`/rest/workflows/${terminal.id}/deactivate`, 'POST', {});
            suite.diagnostic('Native failed batch remains persisted as error, preserves raw Text/pairing and receives terminal-policy ACKs');
            suite.diagnostic(instance.output.split('\n').filter(line => line.startsWith('NATS_TEST_PUBLIC_COMPLETION')).slice(-3).join('\n'));

            const defaultError = await createProcessingWorkflow('terminal-redeliver', [terminalNode]);
            await client.publish('host.terminal-redeliver', Buffer.from('{}'));
            await until(async () => (await manager.consumers.info('HOST', 'terminal-redeliver')).num_redelivered > 0, 30000);
            await instance.request(`/rest/workflows/${defaultError.id}/deactivate`, 'POST', {});
            assert.ok((await executions(defaultError.id)).every(row => row.status === 'error'));

            for (const fault of ['admission', 'persistence'])
            {
                const workflow = await createProcessingWorkflow(`fault-${fault}`, [terminalNode], { onExecutionError: 'acknowledge' });
                const operation = fault === 'admission' ? 'INSERT' : 'UPDATE';
                const statusCondition = fault === 'admission' ? '' : " AND NEW.status = 'error'";
                if (sqlite)
                {
                    sqlite.exec(`CREATE TRIGGER fail_${fault} BEFORE ${operation} ON execution_entity WHEN NEW.workflowId = '${workflow.id}'${statusCondition} BEGIN SELECT RAISE(ABORT, 'Synthetic ${fault} failure'); END;`);
                }
                else
                {
                    await databaseClient.query(`CREATE FUNCTION fail_${fault}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW."workflowId" = '${workflow.id}'${statusCondition} THEN RAISE EXCEPTION 'Synthetic ${fault} failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_${fault} BEFORE ${operation} ON execution_entity FOR EACH ROW EXECUTE FUNCTION fail_${fault}();`);
                }
                await client.publish(`host.fault-${fault}`, Buffer.from('{}'));
                await until(async () => (await manager.consumers.info('HOST', `fault-${fault}`)).num_redelivered > 0, 30000);
                await instance.request(`/rest/workflows/${workflow.id}/deactivate`, 'POST', {});
                if (fault === 'admission') assert.equal((await executions(workflow.id)).length, 0);
            }
            suite.diagnostic('Native admission and failed-result persistence rejection redeliver despite acknowledge policy');

            const retryNode = { id: 'native-retry', name: 'Retry', type: 'n8n-nodes-base.httpRequest', typeVersion: 4.2, position: [200, 0], retryOnFail: true, maxTries: 3, waitBetweenTries: 1000, parameters: { url: `http://127.0.0.1:${slowServer.address().port}/native-retry`, options: {} } };
            const retryWorkflow = await createProcessingWorkflow('native-retry', [retryNode], { maxInFlight: 1, onExecutionError: 'acknowledge' });
            await client.publish('host.native-retry', Buffer.from('{}'));
            await until(async () =>
            {
                const rows = await executions(retryWorkflow.id);
                return rows.length === 1 && rows[0].status === 'success' && (await manager.consumers.info('HOST', 'native-retry')).num_ack_pending === 0;
            }, 30000);
            assert.equal(requests.filter(path => path === '/native-retry').length, 3);
            assert.equal((await manager.consumers.info('HOST', 'native-retry')).num_redelivered, 0);
            await instance.request(`/rest/workflows/${retryWorkflow.id}/deactivate`, 'POST', {});
            suite.diagnostic('Native RetryOnFail completes on third HTTP attempt in one execution without broker redelivery');

            for (const scenario of [
                { suffix: 'terminal-code', node: { id: 'code-failure', name: 'CodeFailure', type: 'n8n-nodes-base.code', typeVersion: 2, position: [200, 0], parameters: { jsCode: "throw new Error('Synthetic Code failure');" } } },
                { suffix: 'terminal-http', node: { ...retryNode, id: 'http-failure', name: 'HttpFailure', parameters: { url: `http://127.0.0.1:${slowServer.address().port}/terminal-http`, options: {} } } },
            ])
            {
                const workflow = await createProcessingWorkflow(scenario.suffix, [scenario.node], { onExecutionError: 'acknowledge' });
                await client.publish(`host.${scenario.suffix}`, Buffer.from('{}'));
                await until(async () =>
                {
                    const rows = await executions(workflow.id);
                    return rows.length === 1 && rows[0].status === 'error' && (await manager.consumers.info('HOST', scenario.suffix)).num_ack_pending === 0;
                }, 30000);
                const execution = await instance.request(`/rest/executions/${(await executions(workflow.id))[0].id}`);
                const data = typeof execution.data === 'string' ? parse(execution.data) : execution.data;
                assert.equal(data.resultData.lastNodeExecuted, scenario.node.name);
                assert.equal(data.resultData.runData[scenario.node.name].at(-1).executionStatus, 'error');
                assert.ok(data.resultData.error);
                if (scenario.suffix === 'terminal-code') assert.match(data.resultData.error.message, /Synthetic Code failure/);
                else assert.equal(requests.filter(path => path === '/terminal-http').length, 3);
                assert.equal((await manager.consumers.info('HOST', scenario.suffix)).num_redelivered, 0);
                await instance.request(`/rest/workflows/${workflow.id}/deactivate`, 'POST', {});
                suite.diagnostic(`${scenario.suffix}: one persisted native error, confirmed terminal-policy ACK and no broker redelivery${scenario.suffix === 'terminal-http' ? ' after three HTTP 401 attempts' : ''}`);
            }

            const manualBatch = await createProcessingWorkflow('manual-batch', [collectNode], { batchSize: 2, onExecutionError: 'acknowledge' }, 'text', false);
            await until(async () => await server.connections() === 1);
            await beginManual(manualBatch, 'manual-batch');
            for (let index = 0; index < 3; index++) await client.publish('host.manual-batch', Buffer.from(`item-${index}`));
            await until(async () =>
            {
                const rows = await executions(manualBatch.id);
                return rows.length === 1 && rows[0].status === 'success' && (await manager.consumers.info('HOST', 'manual-batch')).num_ack_pending === 0;
            }, 30000);
            await assertCleanup();
            assert.equal((await manager.consumers.info('HOST', 'manual-batch')).num_pending, 1);
            suite.diagnostic('Native manual batch consumes one execution only and leaves later messages pending');
            if (process.env.N8N_WORKFLOW_QUALIFIER)
            {
                await require(process.env.N8N_WORKFLOW_QUALIFIER)({
                    request: instance.request, executions, until,
                    diagnostic: message => suite.diagnostic(message),
                    decodeData: data => typeof data === 'string' ? parse(data) : data,
                });
            }
        }
        catch (error)
        {
            error.stack += `\nHost output:\n${instance?.output ?? 'Host startup did not complete'}`;
            throw error;
        }
        finally
        {
            await instance?.close(); sqlite?.close(); await databaseClient?.end(); await postgres?.cluster.stop(); await stop(redis);
            for (const timer of responseTimers) clearTimeout(timer);
            slowServer?.closeAllConnections();
            if (slowServer) await new Promise(resolve => slowServer.close(resolve));
            await connection.close(); await server.close(); await rm(directory, { recursive: true });
        }
    });
}
