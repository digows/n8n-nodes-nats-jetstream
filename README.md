# NATS JetStream for n8n

[![npm version](https://img.shields.io/npm/v/n8n-nodes-nats-jetstream)](https://www.npmjs.com/package/n8n-nodes-nats-jetstream)
[![CI](https://github.com/digows/n8n-nodes-nats-jetstream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/digows/n8n-nodes-nats-jetstream/actions/workflows/ci.yml)
[![MIT license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Connect self-hosted n8n workflows to NATS JetStream with durable message consumption and confirmed publishing.** `n8n-nodes-nats-jetstream` is an open-source TypeScript community node package for event-driven workflow automation, using the official NATS JavaScript SDK.

Use it to trigger workflows from broker subjects, distribute work across a shared durable consumer, or publish JSON, text and binary messages to a JetStream stream. Version [0.1.0 is available on npm](https://www.npmjs.com/package/n8n-nodes-nats-jetstream/v/0.1.0). This is an **unverified community package for self-hosted n8n**, not an n8n Cloud verified integration.

## Features

- Durable pull consumer trigger with broker-side subject filters and bounded in-flight delivery.
- Confirmed consumer acknowledgements after workflow success or a persisted n8n Wait handoff, not merely after emitting a message.
- JSON, UTF-8 text and binary publishing with broker acknowledgements, headers and caller-supplied message IDs.
- Native NATS username/password, token, NKey and `.creds` authentication.
- Verified TLS, custom certificate authorities and mutual TLS (mTLS).
- Existing-consumer binding by default, with explicit create-if-missing mode and reconnect handling.

At-least-once delivery requires idempotent business effects. Read [completion and redelivery](#completion-and-redelivery), especially the manual-mode deadline, before consuming a production stream.

## Installation

Prerequisites: self-hosted n8n, Node.js 24 or newer, a reachable NATS server with JetStream enabled, and an existing stream. Consumer creation requires nats-server 2.10 or newer. Broker permissions are described [below](#broker-permissions).

### Install from the n8n editor

As an n8n Owner or Admin:

1. Open **Settings → Community Nodes → Install**.
2. Enter `n8n-nodes-nats-jetstream@0.1.0` to select the published version explicitly.
3. Review the community-node risks and select **Install**.
4. Find **NATS JetStream Trigger** and **NATS JetStream** in the node picker.

Follow the official [n8n GUI installation guide](https://docs.n8n.io/integrations/community-nodes/installation-and-management/gui-installation.md). Queue-mode deployments require the official [manual installation procedure](https://docs.n8n.io/integrations/community-nodes/installation-and-management/manual-installation.md), with the package available to the main process and workers. Running `npm install` in an unrelated project directory does not install a node into n8n.

The functional regular/queue host suites were measured on n8n 2.42.3. Package registration and startup were subsequently verified in a custom n8n 2.42.6 image; that is not a rerun of the functional host suite or proof of every native installer path. See [validation evidence](docs/validation.md) for the measured scope and installation caveat.

## Quick start

### Trigger a workflow from a JetStream message

1. Create a **NATS JetStream API** credential with your server address and authentication settings. Keep secrets in the credential, not the workflow payload.
2. Add **NATS JetStream Trigger**. Set **Stream** and **Durable Consumer** to a durable pull consumer configured with explicit acknowledgements.
3. Keep **Consumer Mode → Bind Existing** to avoid changing the broker configuration. Set **Payload Format → JSON** for JSON messages.
4. Use `{{ $json.payload }}` to read the message payload in downstream nodes and `{{ $json.metadata.subject }}` to read its subject.

Use a dedicated test consumer for manual executions: testing consumes real messages and can advance its cursor.

### Publish a JSON message

Add **NATS JetStream**, select the same credential, and configure:

| Field | Example |
| --- | --- |
| Subject | `orders.created` |
| Payload Format | `JSON` |
| JSON Payload, expression mode | `{{ $json }}` |
| Options → Message ID | A stable identifier for the logical event, reused on retries |
| Options → Expected Stream | The stream that should store the message, when required |

The subject must match an existing stream. The node returns the broker's `stream`, `sequence` and `duplicate` result; it does not claim that a consumer has processed the message. See [output and publishing](#output-and-publishing) for formats, expressions and retry ambiguity.

## Nodes

- **NATS JetStream Trigger** consumes a named durable pull consumer. A message is acknowledged only after the parent workflow succeeds or n8n persists a resumable Wait execution.
- **NATS JetStream** publishes JSON, UTF-8 text or an input binary field, waits for the broker's publication acknowledgement, and returns one paired result per input item.

The package uses the official NATS SDK 3.4.0. No application-specific routing, automatic dead-letter queue, broker deployment or stream administration is included. Core NATS subscriptions, business request/reply services, KV and Object Store are outside this package's scope.

## Credentials

Use the **NATS JetStream API** credential for both nodes. Enter one or more server addresses separated by commas or whitespace, without embedded authentication. Optionally configure a connection name, connection timeout and JetStream domain.

Select None, Username/Password, Token, NKey private seed, or complete `.creds` content. These are native NATS authentication mechanisms: an OIDC access token is not automatically a NATS user JWT. Required credentials never fall back to anonymous authentication.

TLS verifies the server certificate. Supply a custom CA when necessary and both client certificate and private key for mTLS. There is no option to disable verification. All private material belongs in n8n credentials, not workflow parameters.

The credential test connects, flushes and closes without publishing, consuming or creating broker resources. Success proves connectivity/authentication only, not node-operation permissions.

## Durable consumer setup

Create the stream outside this package. **Bind Existing** is the default consumer mode; it reads the selected consumer and never changes its cursor or configuration. The consumer must be durable, pull-based and use `AckExplicit`.

**Create If Missing** explicitly permits creation. Configure subject filters, New/All delivery policy, ACK wait in milliseconds, maximum delivery attempts and maximum pending acknowledgements. An existing consumer must match these settings; incompatible configuration fails without updating it. Concurrent creation is followed by a fresh compatibility check. No consumer is deleted on shutdown.

One or multiple subject filters are evaluated by the broker, not against JSON values. Creation requires nats-server 2.10 or newer: older servers do not enforce the create-only action. Bind Existing does not create/update a consumer. Servers without multiple-filter support fail explicitly. Replicas sharing a durable distribute messages; separate consumers follow the stream's retention policy. Work-queue retention is not a fan-out guarantee.

The local in-flight limit includes fetched messages, queued executions, running workflows and unconfirmed ACKs. Pulls are bounded by remaining capacity. Progress notifications keep messages alive while healthy processing takes longer than ACK wait, including consumers configured with BackOff.

## Completion and redelivery

`emit()` and queue admission do not acknowledge a message. Successful parent execution or persisted Wait handoff leads to `ackAck()` with confirmation. Failed admission/execution, unknown results and cancellation release the message through delayed NAK when connected, or leave it unacknowledged when disconnected.

After a persisted Wait handoff, n8n owns the resumable execution. Restart/resume does not require an in-memory trigger callback. Later failure follows n8n's error handling, not NATS redelivery.

An unconfirmed ACK is reported as an unknown outcome, without sending another NAK or asserting whether the broker accepted it. Malformed payloads are recorded as trigger failures and redelivered under the same policy. MaxDeliver exhaustion requires operator handling; this package does not construct a dead-letter queue.

Shutdown stops pulls, allows in-flight work the configured grace period, then closes the connection and releases unfinished work. SDK reconnect retains the same durable. At-least-once delivery is not exactly-once HTTP/database effects; provide idempotency where business effects occur. Continue On Fail and detached subworkflows can make the parent's reported outcome differ from a child's outcome.

**Approved manual-mode limitation:** n8n 2.42.3 invokes the manual close callback immediately after trigger output, before workflow completion. The listener stops immediately; only the emitted delivery remains alive for the configured shutdown grace period (default 30,000 ms from trigger output). Success or persisted Wait within that period permits confirmed ACK. Cancellation, persistence failure or expiration releases without success ACK and closes the connection/progress timers. Expiration does not cancel the downstream workflow: it can finish successfully after its message is already available for redelivery, allowing duplicate effects. This is an effective manual delivery-settlement deadline, not an automatic-workflow runtime limit. Manual testing consumes real messages from the selected durable; it is not a non-consuming preview. Use a dedicated test consumer.

## Output and publishing

JSON/Text trigger output is `{ "payload": ..., "metadata": ... }`. JSON objects, arrays and scalar values stay inside `payload`. Metadata includes subject, stream, consumer, decimal-string sequences, delivery count, redelivery flag, timestamp, nullable message ID and repeated headers as string arrays.

Binary output uses n8n's `binary.data` helper and metadata in `json`; it does not put bytes/base64 in `json.payload`. Select Text or Binary if normal JavaScript JSON-number semantics do not preserve the representation you need. Unsafe SDK numeric sequences are rejected rather than emitted after rounding.

Publisher subjects are literal NATS subjects and may be expressions per item; subscription wildcards are invalid. Fixed JSON editor text is parsed before serialization. Expressions provide evaluated JSON values and preserve their types: `={{ "123" }}` publishes a JSON string, not a number, and `={{ "hello" }}` is valid. An expression returning serialized JSON text still returns a string; return an object/array or explicitly parse that text in the expression to publish its contents as JSON. The resulting value is serialized exactly once. Text is UTF-8. Binary uses the selected input binary field through n8n's helper, including external binary storage.

Optional headers accept strings or string arrays. Dedicated message ID/expected stream values must not conflict with their reserved headers. Reuse a caller-supplied stable message ID on retries; publication deduplication lasts only within the stream's configured duplicate window. No automatic payload hash or random retry ID is generated.

Results contain `subject`, `stream`, decimal-string `sequence`, `duplicate`, nullable `messageId` and `pairedItem`. A duplicate is a successful broker acknowledgement. Results describe storage, not consumer processing. A publication timeout is ambiguous: the broker may have stored the message despite a lost reply.

## Broker permissions

Permissions remain broker-owned. The package does not require an account-wide `$JS.API.INFO` probe. SDK 3.4.0 uses these subjects (replace stream, consumer and domain with your deployment values):

| Operation | Client permission |
| --- | --- |
| Publication | Publish to each application subject; subscribe to response `_INBOX.>` subjects. |
| Bind/info | Publish `$JS.API.CONSUMER.INFO.<stream>.<consumer>`. |
| Pull | Publish `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>`; subscribe to response `_INBOX.>` subjects. |
| ACK/progress/NAK | Publish the broker-provided `$JS.ACK.>` reply subjects, scoped to the selected consumer where possible; confirmed ACK also requires response inbox subscription. |
| Explicit creation, single/no filter | Publish `$JS.API.CONSUMER.CREATE.<stream>.<consumer>` and, for a single filter, its `.<filter>` suffix. |
| Explicit creation, multiple filters | Publish `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.<consumer>`. |

A configured domain changes the JetStream API prefix to `$JS.<domain>.API`; ACK reply subjects are broker-provided and may include domain/account tokens. Use broker policy appropriate to those actual reply subjects. Creation permission is unnecessary in Bind Existing mode. Subject filtering is not an authorization boundary.

## Development and validation

Use Node.js 24 and the lockfile:

```sh
npm ci
npm run check
```

`check` runs the official node build/lint, TypeScript checks and deterministic unit tests. Broker tests require dedicated native server binaries and the official JWT fixture generator:

```sh
NATS_SERVER_BIN=/absolute/path/to/nats-server-2.15.1 \
NATS_OLD_SERVER_BIN=/absolute/path/to/nats-server-2.9.25 \
NATS_JWT_MODULE=/absolute/path/to/@nats-io/jwt/cjs/jwt.js \
npm run test:broker
```

Actual host tests require an isolated n8n 2.42.3 installation, the built package installed in its community-node directory, embedded-postgres and redis-server:

```sh
N8N_TEST_CLI=/absolute/path/to/n8n/bin/n8n \
N8N_TEST_NODES_DIRECTORY=/absolute/path/to/community/nodes \
EMBEDDED_POSTGRES_MODULE=/absolute/path/to/embedded-postgres/dist/index.js \
REDIS_SERVER_BIN=/absolute/path/to/redis-server \
NATS_SERVER_BIN=/absolute/path/to/nats-server-2.15.1 \
npm run test:host
```

The host suite passes in regular and queue modes, including cancellation, persisted Wait, persistence rejection and processing beyond the approved manual grace period. Tests create disposable data under `/tmp` and never use production brokers or credentials. Use `npm pack --ignore-scripts --pack-destination /tmp/<task-directory>` to build an installation artifact; this does not publish to npm or deploy a runtime.

See [the approved specification](docs/specification.md) for authentication, consumer lifecycle, publishing, Wait handling, acknowledgement guarantees, and acceptance tests.

See [CI and npm releases](docs/releases.md) for validation workflows, npm credentials/OIDC setup, SemVer tags and publication. CI does not publish ordinary pushes or pull requests.

## Documentation and support

- [Package specification](docs/specification.md): authentication, delivery guarantees and public API boundaries.
- [Validation evidence](docs/validation.md): exact versions, measured tests and known limitations.
- [CI and npm releases](docs/releases.md): versioned publication and provenance.
- [Contributing](CONTRIBUTING.md): development setup and reporting reproducible problems.
- [GitHub issues](https://github.com/digows/n8n-nodes-nats-jetstream/issues): bug reports and feature requests.
- [npm package](https://www.npmjs.com/package/n8n-nodes-nats-jetstream): published versions and installation metadata.

Maintained by [Rodrigo P. Fraga](https://github.com/digows). This repository is not an official n8n or NATS project. Licensed under [MIT](LICENSE), including commercial use.
