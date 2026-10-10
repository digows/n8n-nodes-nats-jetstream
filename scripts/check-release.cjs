const assert = require('node:assert/strict');
const { readFileSync, appendFileSync } = require('node:fs');
const { join } = require('node:path');

const releaseVersionPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?$/;

function validateRelease(tag, packageMetadata, lockMetadata)
{
    assert.equal(typeof tag, 'string', 'Set RELEASE_TAG to a version tag such as v0.1.0');
    assert.ok(tag.startsWith('v'), 'Release tags must start with v');
    const version = tag.slice(1);
    assert.ok(releaseVersionPattern.test(version) && version.trim() === version,
        'Release tags must contain canonical SemVer without build metadata');
    assert.equal(packageMetadata.name, 'n8n-nodes-nats-jetstream', 'Unexpected package name');
    assert.equal(packageMetadata.version, version, 'Release tag and package.json version must match');
    assert.equal(lockMetadata.name, packageMetadata.name, 'Lockfile package name must match');
    assert.equal(lockMetadata.version, version, 'Lockfile version must match');
    assert.equal(lockMetadata.packages?.['']?.name, packageMetadata.name, 'Lockfile root name must match');
    assert.equal(lockMetadata.packages?.['']?.version, version, 'Lockfile root version must match');
    return { version, npmTag: version.includes('-') ? 'next' : 'latest' };
}

if (require.main === module)
{
    const repositoryDirectory = join(__dirname, '..');
    const packageMetadata = JSON.parse(readFileSync(join(repositoryDirectory, 'package.json'), 'utf8'));
    const lockMetadata = JSON.parse(readFileSync(join(repositoryDirectory, 'package-lock.json'), 'utf8'));
    const release = validateRelease(process.env.RELEASE_TAG, packageMetadata, lockMetadata);
    if (process.env.GITHUB_OUTPUT)
    {
        appendFileSync(process.env.GITHUB_OUTPUT, `version=${release.version}\nnpm-tag=${release.npmTag}\n`);
    }
    console.log(`Validated release ${release.version}, npm dist-tag ${release.npmTag}`);
}

module.exports = { validateRelease };
