# n8n-nodes-jetstream-nats

Standalone NATS JetStream consumer and publisher community nodes for n8n.

This package implements the approved contract and passes the stated target-host checks. It has not been published to npm or deployed. See [validation evidence](docs/validation.md), including the approved manual-mode limitation.

## Nodes

- **JetStream NATS Trigger** consumes a named durable pull consumer. A message is acknowledged only after the parent workflow succeeds or n8n persists a resumable Wait execution.
- **JetStream NATS** publishes JSON, UTF-8 text or an input binary field, waits for the broker's publication acknowledgement, and returns one paired result per input item.

The package uses the official NATS SDK 3.4.0. The validation target is self-hosted n8n 2.42.3 on Node.js 24. It does not claim compatibility with other n8n versions or n8n Cloud verification. No application-specific routing, automatic dead-letter queue, broker deployment or stream administration is included.

## Credentials

Use the **JetStream NATS API** credential for both nodes. Enter one or more server addresses separated by commas or whitespace, without embedded authentication. Optionally configure a connection name, connection timeout and JetStream domain.

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

Publisher subjects are literal NATS subjects and may be expressions per item; subscription wildcards are invalid. JSON parameters accept a JSON value or JSON text and serialize it once. Text is UTF-8. Binary uses the selected input binary field through n8n's helper, including external binary storage.

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

Licensed under [MIT](LICENSE).
