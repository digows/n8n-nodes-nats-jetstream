const assert = require('node:assert/strict');
const { test } = require('node:test');
const { validateRelease } = require('../../scripts/check-release.cjs');
const packageMetadata = require('../../package.json');
const lockMetadata = require('../../package-lock.json');

function metadata(version = '0.1.0')
{
    const packageMetadata = { name: 'n8n-nodes-nats-jetstream', version };
    const lockMetadata = { ...packageMetadata, packages: { '': { ...packageMetadata } } };
    return { packageMetadata, lockMetadata };
}

test('stable versions target latest; prereleases target next', () =>
{
    for (const version of ['0.1.0', '1.2.3', '10.20.30', '1.0.0-rc.1', '0.2.0-beta.0', '1.0.0-0', '1.0.0-alpha-1'])
    {
        const { packageMetadata, lockMetadata } = metadata(version);
        assert.deepEqual(validateRelease(`v${version}`, packageMetadata, lockMetadata),
            { version, npmTag: version.includes('-') ? 'next' : 'latest' });
    }
});

test('release tags reject malformed versions and noncanonical numeric identifiers', () =>
{
    const { packageMetadata, lockMetadata } = metadata();
    for (const tag of [undefined, '', '0.1.0', 'v1', 'v1.2', 'v01.2.3', 'v1.02.3', 'v1.2.03',
        'v1.2.3-', 'v1.2.3-01', 'v1.2.3-rc.01', 'v1.2.3-rc..1', 'v1.2.3+build', 'v1.2.3\n', 'v1.2.3;command'])
    {
        assert.throws(() => validateRelease(tag, packageMetadata, lockMetadata));
    }
});

test('tag, package and both lockfile versions must agree', () =>
{
    const { packageMetadata, lockMetadata } = metadata();
    assert.throws(() => validateRelease('v0.2.0', packageMetadata, lockMetadata), /must match/);
    assert.throws(() => validateRelease('v0.1.0', packageMetadata, { ...lockMetadata, version: '0.2.0' }), /must match/);
    assert.throws(() => validateRelease('v0.1.0', packageMetadata,
        { ...lockMetadata, packages: { '': { ...packageMetadata, version: '0.2.0' } } }), /must match/);
    assert.throws(() => validateRelease('v0.1.0', packageMetadata, { ...lockMetadata, packages: {} }), /must match/);
    assert.throws(() => validateRelease('v0.1.0', { ...packageMetadata, name: 'wrong-package' }, lockMetadata), /Unexpected/);
    assert.throws(() => validateRelease('v0.1.0', packageMetadata, { ...lockMetadata, name: 'wrong-package' }), /must match/);
});

test('repository metadata and compiled node/credential entries are consistent', () =>
{
    validateRelease(`v${packageMetadata.version}`, packageMetadata, lockMetadata);
    assert.equal(packageMetadata.repository.url, 'git+https://github.com/digows/n8n-nodes-nats-jetstream.git');
    assert.equal(packageMetadata.homepage, 'https://github.com/digows/n8n-nodes-nats-jetstream');
    assert.equal(packageMetadata.bugs.url, 'https://github.com/digows/n8n-nodes-nats-jetstream/issues');
    assert.ok(packageMetadata.keywords.includes('n8n-community-node-package'));
    for (const keyword of ['n8n', 'nats', 'jetstream', 'workflow-automation'])
    {
        assert.ok(packageMetadata.keywords.includes(keyword));
    }
    assert.equal(new Set(packageMetadata.keywords).size, packageMetadata.keywords.length);
    assert.ok(packageMetadata.files.includes('CONTRIBUTING.md'));
    assert.deepEqual(packageMetadata.n8n.nodes, [
        'dist/nodes/NatsJetstream/NatsJetstream.node.js',
        'dist/nodes/NatsJetstream/NatsJetstreamTrigger.node.js',
    ]);
    assert.deepEqual(packageMetadata.n8n.credentials, ['dist/credentials/NatsJetstreamApi.credentials.js']);
    const { NatsJetstream } = require('../../dist/nodes/NatsJetstream/NatsJetstream.node.js');
    const { NatsJetstreamTrigger } = require('../../dist/nodes/NatsJetstream/NatsJetstreamTrigger.node.js');
    const { NatsJetstreamApi } = require('../../dist/credentials/NatsJetstreamApi.credentials.js');
    assert.equal(new NatsJetstream().description.name, 'natsJetstream');
    assert.equal(new NatsJetstreamTrigger().description.name, 'natsJetstreamTrigger');
    assert.equal(new NatsJetstreamApi().name, 'natsJetstreamApi');
});
