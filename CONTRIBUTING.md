# Contributing to NATS JetStream for n8n

Thank you for contributing to `n8n-nodes-nats-jetstream`. Read the [README](README.md) and [package specification](docs/specification.md) before proposing changes to message delivery, credentials or consumer behavior.

## Report a problem or request a feature

Use [GitHub issues](https://github.com/digows/n8n-nodes-nats-jetstream/issues). For a reproducible bug report, include:

- Package, n8n, Node.js and nats-server versions.
- Regular or queue mode and the installation method.
- Expected behavior, observed behavior and minimal reproduction steps.
- Relevant logs and consumer configuration with credentials and sensitive payloads removed.

Do not include access tokens, NKey seeds, `.creds` files, private keys or production personal data. Describe a feature's use case and how it fits the existing package scope.

## Develop and validate a change

Use Node.js 24 and the repository lockfile:

```sh
npm ci
npm run check
```

`check` runs the official n8n node build and lint, TypeScript checks and unit tests. Add or update regression tests for behavior changes. Use the [README's broker and host test instructions](README.md#development-and-validation) when a change affects authentication, delivery, publishing or n8n lifecycle integration. Tests must use dedicated fixtures, not production streams or credentials.

Open a pull request against `main`, explain the change and report the exact commands and results you measured. Keep documentation-only claims distinct from functional test evidence. Runtime code uses public n8n interfaces and the official NATS SDK; do not introduce private n8n core imports or acknowledge messages before the documented success/Wait boundary.

## Releases and license

Ordinary pushes and pull requests run validation without publishing to npm. Release tags and published versions are immutable; follow the [release procedure](docs/releases.md) rather than moving tags or reusing a published version.

This project is licensed under [MIT](LICENSE). No new support response-time or compatibility guarantee is implied by these contribution instructions.
