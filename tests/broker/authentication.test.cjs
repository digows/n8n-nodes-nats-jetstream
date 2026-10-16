const assert = require('node:assert/strict');
const { test } = require('node:test');
const { execFileSync } = require('node:child_process');
const { mkdtemp, writeFile, readFile, rm } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { nkeys, connect } = require('@nats-io/transport-node');
const { jetstreamManager, AckPolicy, StorageType } = require('@nats-io/jetstream');
const { broker } = require('../helpers.cjs');
const { openConnection, connectionOptions } = require('../../dist/lib/connection');
const { bindConsumer } = require('../../dist/lib/consumer');

test('native token and NKey authentication reject malformed or incorrect credentials', async () =>
{
    const tokenServer = await broker('authorization { token: "synthetic-token" }');
    try
    {
        const connection = await openConnection({ servers: tokenServer.url, authentication: 'token', token: 'synthetic-token' });
        await connection.flush(); await connection.close();
        await assert.rejects(openConnection({ servers: tokenServer.url, authentication: 'token', token: 'incorrect' }));
    }
    finally { await tokenServer.close(); }
    const user = nkeys.createUser();
    const nkeyServer = await broker(`authorization { users: [{nkey: "${user.getPublicKey()}"}] }`);
    try
    {
        const connection = await openConnection({ servers: nkeyServer.url, authentication: 'nkey', seed: Buffer.from(user.getSeed()).toString() });
        await connection.flush(); await connection.close();
        await assert.rejects(openConnection({ servers: nkeyServer.url, authentication: 'nkey', seed: 'invalid-private-seed' }));
    }
    finally { user.clear(); await nkeyServer.close(); }
});

test('complete .creds authenticates against an operator/JWT server', async () =>
{
    assert.ok(process.env.NATS_JWT_MODULE, 'Set NATS_JWT_MODULE to the official @nats-io/jwt package entry');
    const jwt = require(process.env.NATS_JWT_MODULE);
    const directory = await mkdtemp(join(tmpdir(), 'jetstream-jwt-'));
    const operator = jwt.createOperator(), account = jwt.createAccount(), user = jwt.createUser(), systemAccount = jwt.createAccount();
    let server;
    try
    {
        const operatorJwt = await jwt.encodeOperator('Test Operator', operator, { system_account: systemAccount.getPublicKey() });
        const limits = { conn: -1, subs: -1, data: -1, payload: -1 };
        const accountJwt = await jwt.encodeAccount('Test Account', account, { limits }, { signer: operator });
        const systemJwt = await jwt.encodeAccount('System Account', systemAccount, { limits }, { signer: operator });
        const userJwt = await jwt.encodeUser('Test User', user, account);
        const operatorPath = join(directory, 'operator.jwt');
        await writeFile(operatorPath, operatorJwt);
        server = await broker(`operator: "${operatorPath}"\nresolver: MEMORY\nresolver_preload { ${account.getPublicKey()}: "${accountJwt}"; ${systemAccount.getPublicKey()}: "${systemJwt}" }`);
        const credentials = { servers: server.url, authentication: 'creds', creds: Buffer.from(jwt.fmtCreds(userJwt, user)).toString() };
        const authentication = connectionOptions(credentials).authenticator('test-nonce');
        assert.equal(authentication.nkey, user.getPublicKey());
        assert.equal(authentication.jwt, userJwt);
        const connection = await openConnection(credentials);
        await connection.flush(); await connection.close();
        const withoutTrailingNewline = await openConnection({ ...credentials, creds: credentials.creds.trimEnd() });
        await withoutTrailingNewline.flush(); await withoutTrailingNewline.close();
        await assert.rejects(openConnection({ servers: server.url, authentication: 'creds', creds: 'invalid-file-content' }));
    }
    catch (error) { error.stack += `\nBroker output: ${server?.output}`; throw error; }
    finally { await server?.close(); operator.clear(); account.clear(); user.clear(); systemAccount.clear(); await rm(directory, { recursive: true }); }
});

test('TLS verifies the server CA and supports client certificates without insecure fallback', async () =>
{
    const directory = await mkdtemp(join(tmpdir(), 'jetstream-tls-'));
    const certificatePath = join(directory, 'certificate.pem'), keyPath = join(directory, 'key.pem');
    let server;
    try
    {
        execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyPath, '-out', certificatePath, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'], { stdio: 'ignore' });
        const certificate = await readFile(certificatePath, 'utf8'), privateKey = await readFile(keyPath, 'utf8');
        server = await broker(`tls { cert_file: "${certificatePath}"; key_file: "${keyPath}"; ca_file: "${certificatePath}"; verify: true }`);
        const credentials = { servers: server.url, authentication: 'none', tls: true, ca: certificate, clientCertificate: certificate, clientKey: privateKey };
        const connection = await openConnection(credentials); await connection.flush(); await connection.close();
        await assert.rejects(openConnection({ ...credentials, ca: '' }));
        await assert.rejects(openConnection({ servers: server.url, authentication: 'none', tls: true, ca: certificate }));
    }
    finally { await server?.close(); await rm(directory, { recursive: true }); }
});

test('bind existing uses only consumer permissions, not creation or account-wide API access', async () =>
{
    const server = await broker(`authorization { users: [
        {user: "admin", password: "admin-test"},
        {user: "consumer", password: "consumer-test", permissions: {
            publish: { allow: ["$JS.API.CONSUMER.INFO.TESTS.least", "$JS.API.CONSUMER.MSG.NEXT.TESTS.least", "$JS.ACK.TESTS.least.>"] },
            subscribe: { allow: ["_INBOX.>"] }
        }}
    ] }`);
    let administrator, limited;
    try
    {
        administrator = await connect({ servers: server.url, user: 'admin', pass: 'admin-test' });
        const manager = await jetstreamManager(administrator);
        await manager.streams.add({ name: 'TESTS', subjects: ['tests.>'], storage: StorageType.File });
        await manager.consumers.add('TESTS', { durable_name: 'least', ack_policy: AckPolicy.Explicit });
        limited = await openConnection({ servers: server.url, authentication: 'userPassword', username: 'consumer', password: 'consumer-test' });
        const { info } = await bindConsumer(limited, { stream: 'TESTS', durable: 'least', mode: 'existing' });
        assert.equal(info.name, 'least');
        await assert.rejects(bindConsumer(limited, { stream: 'TESTS', durable: 'not-allowed', mode: 'create', configuration: { durable_name: 'not-allowed', ack_policy: AckPolicy.Explicit } }));
        assert.equal((await manager.streams.info('TESTS')).state.consumer_count, 1);
    }
    finally { await limited?.close(); await administrator?.close(); await server.close(); }
});
