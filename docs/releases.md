# CI and npm releases

This is an unverified community package for self-hosted n8n. Publishing to npm does not grant n8n Cloud verification. Runtime dependencies on the official NATS SDK remain unchanged.

## Validation

`.github/workflows/ci.yml` runs on pushes to `main`, pull requests targeting `main`, and calls from the publish workflow. It uses Node.js 24.21.0, `npm ci`, official build/lint, typechecking, unit tests, and all broker tests against checksum-pinned NATS 2.15.1 and 2.9.25 with the official JWT fixture generator 0.0.10-5. It also previews the npm package and checks that validation did not modify package metadata or the lockfile. Third-party Actions are pinned to full commit hashes.

The actual n8n regular/queue host suite is not run by this CI; its isolated n8n/PostgreSQL/Redis setup and measured results remain documented in [validation](validation.md). CI success is not evidence of a fresh host-suite run.

## One-time publishing setup

You need an npm account authorized to publish `n8n-nodes-nats-jetstream`. Configure the GitHub environment `npm` with any required reviewer and tag deployment restrictions before releasing.

For the first publication, configure a granular npm token with publish permission as `NPM_TOKEN` in that environment. Configure its 2FA/bypass policy appropriately for noninteractive publishing; never commit the token or place it in a workflow file. The first publication also reserves the package name, which must be available or owned by your npm account.

After the package exists, prefer npm Trusted Publishing instead of a long-lived token. In the npm package settings authorize:

- GitHub owner: `digows`
- Repository: `n8n-nodes-nats-jetstream`
- Workflow filename: `publish.yml`
- Environment: `npm`
- Allow direct `npm publish`.

Remove `NPM_TOKEN` after OIDC is configured. The publish job has `id-token: write` and runs on a GitHub-hosted runner; Node 24.21.0 includes an npm version supporting Trusted Publishing. Publishing includes provenance. See [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) and [provenance](https://docs.npmjs.com/generating-provenance-statements/).

## Versioning and release

Package versions use SemVer and Git tags use `v<version>`. `package.json`, the top-level lockfile version and its root package version must all match the tag. Stable versions publish to `latest`; prereleases such as `v0.2.0-rc.1` publish to `next`. Noncanonical versions and build metadata are rejected. Published versions are immutable: a changed package requires a new version, not a moved/reused release tag.

The workflow only publishes a tag whose commit belongs to `main`, after running the full CI against that tag. Publishing is separate from ordinary pushes and PRs and has no manual-dispatch trigger. No package version or release tag was created by the CI configuration change.

For the first, currently unpublished version, after configuring npm access and confirming the `main` CI is green:

```sh
git switch main
git pull --ff-only
RELEASE_TAG=v0.1.0 node scripts/check-release.cjs
git tag -a v0.1.0 -m "Release 0.1.0"
git push origin v0.1.0
```

For subsequent releases, choose the version deliberately rather than automatically bumping on every CI run. For example:

```sh
npm version patch -m "chore(release): %s"
git push origin main
git push origin v0.1.1
```

Use `minor`/`major` when appropriate, or an explicit prerelease version. Review the resulting commit and exact tag before pushing. Wait for the publishing run and check the public npm version/provenance before announcing the release. This workflow neither deploys n8n nor modifies production workflows.
