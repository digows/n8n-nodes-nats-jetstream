# n8n-nodes-nats-jetstream

## Purpose and scope

Build a small, standalone n8n community package for reliable NATS JetStream consumption and publication. It must work with arbitrary subjects and payloads, without application-specific rules, endpoints, schemas, authentication services, or infrastructure assumptions.

The approved first version contains two programmatic nodes and one credential type:

| Component | Internal name | Responsibility |
| --- | --- | --- |
| NATS JetStream Trigger | `natsJetstreamTrigger` | Consume an existing stream through a named durable pull consumer. |
| NATS JetStream | `natsJetstream` | Publish messages and return the broker's publication acknowledgement. |
| NATS JetStream credential | `natsJetstreamApi` | Connection, native NATS authentication, and TLS settings. |

Use the official NATS JavaScript SDK, not a new protocol implementation or a fork of a larger integration package. The package license is MIT, including commercial use.

Out of scope: Core NATS subscriptions, request/reply business services, stream administration, KV, Object Store, application event generation, payload-based routing, automatic dead-letter queues, external idempotency stores, and runtime deployment.

## Upstream contracts and compatibility

Initial n8n validation target: **n8n 2.42.3, Node.js 24**. Use public `n8n-workflow` interfaces; do not import `n8n-core`, CLI internals, execution repositories, Redis queues, or private lifecycle hooks.

The published package must follow the official community-node contract:

- Package name `n8n-nodes-nats-jetstream` and keyword `n8n-community-node-package`.
- `n8n.n8nNodesApiVersion: 1`, with compiled node and credential paths.
- Node `version: 1`, stable internal names, correct trigger inputs/outputs, and bundled icons.
- `peerDependencies: { "n8n-workflow": "*" }`, as required by the official community-node lint rule. Use a development dependency compatible with the validation target.
- Compile with the official `@n8n/node-cli`; include only required runtime dependencies and distribution assets.
- Maintain a lockfile. The inspected NATS SDK packages are `@nats-io/transport-node` and `@nats-io/jetstream`, version **3.4.0**. Resolve and test the actual dependency versions during implementation; do not silently substitute an older client.

Validate against a real JetStream server and record its exact version. Existing-consumer and consumer-creation modes need separate tests. Multiple subject filters must fail clearly on servers that do not support them, not silently reduce to one filter. Do not claim support for untested n8n versions or n8n Cloud verification.

## Credentials and authentication

All three components use n8n's normal credential storage. Secrets belong in credentials, not node parameters, workflow JSON, message output, or log entries.

Expose these connection settings:

- One or more server addresses, normalized into the SDK's server list.
- Optional connection name and connection timeout.
- Optional JetStream domain for deployments that use one.
- A selected authentication mode, independent of TLS.

Supported authentication modes:

| Mode | Credential values | SDK mechanism |
| --- | --- | --- |
| None | No protocol credential | Explicit anonymous connection when the server permits it. |
| Username/password | Username and password | Native NATS username/password authentication. |
| Token | NATS token | Native token authentication. |
| NKey | Private seed | SDK NKey challenge signing. |
| Credentials | Complete `.creds` content | SDK credentials authenticator: NATS user JWT and private seed. |

Accept `.creds` content directly, not a required filesystem path. Required values for the selected mode must be validated before connecting. A malformed credential or failed authentication must never fall back to anonymous access. Use the SDK authenticators; do not implement JWT signing or NKey cryptography.

TLS supports normal server verification, an optional custom CA, and an optional client certificate/private key pair for mTLS. Accept PEM content in credentials. Do not introduce a switch that disables certificate verification. Protocol authentication and certificate-based authentication can coexist according to server configuration.

A Keycloak/OIDC access token is not automatically a NATS user JWT. Any external identity integration remains a server/operator concern, not a dependency of this library.

Implement a programmatic n8n credential test: connect, flush, and close within a timeout. It must not publish, consume, ACK, create a consumer, or administer a stream. A passing test proves connectivity/authentication only, not permission to perform every node operation.

NATS permissions remain broker-owned. Document the permissions actually used by the SDK for publishing, consumer information, pulls, ACKs, and response inboxes. Consumer creation requires additional permissions only when explicitly selected. Avoid an unnecessary account-wide API probe when connecting a least-privilege consumer.

## Trigger configuration

Required: credential, stream name, durable consumer name, and payload format (`JSON`, `Text`, or `Binary`). A named durable consumer is always required; never implicitly obtain an ordered ephemeral consumer.

Two consumer modes:

1. **Bind existing** (default): read consumer information and use its existing cursor/configuration. Do not require creation permissions or change its delivery policy or filters.
2. **Create if missing**: explicitly create a durable pull consumer if absent. If already present, compare the requested configuration and bind only when compatible; never overwrite it.

Creation settings are subject filters, delivery policy (`New` by default, or `All` when selected), `AckWait`, `MaxDeliver`, and `MaxAckPending`. Use `AckExplicit`. Expose durations in milliseconds or seconds with explicit units and convert to SDK/server units at the boundary.

`MaxAckPending` defaults to `1000`. Accept `-1` for unlimited pending acknowledgements across the durable consumer, or a positive safe integer for a finite limit; reject zero and other invalid values. This is independent of the local maximum in-flight setting, which remains positive and defaults to `1` per trigger instance. Omitting the creation setting retains `1000`, not unlimited. Existing-consumer mismatch detection and the no-update contract remain unchanged.

Use the SDK's create-only consumer operation. When another activation wins the creation race, re-read and compare the consumer. Authorization errors, invalid configuration, and connectivity failures are not evidence that a consumer is missing.

An existing consumer must be durable, pull-based, and use explicit acknowledgements. Incompatible push, ordered, or `AckAll`/`AckNone` consumers must fail activation with an actionable message. No consumer is deleted or recreated on activation, deactivation, or deployment.

Subject filtering is server-side. One or multiple filters select messages by subject; they are not JSON predicates or security boundaries. Replicas sharing a durable consumer share the work; different durable consumers receive independent deliveries, subject to stream retention policy. Do not promise fan-out where the stream uses work-queue retention.

Runtime controls are local maximum in-flight messages, pull timeout, delayed-NAK interval, acknowledgement timeout, and shutdown grace period. Keep the normal UI small; advanced settings belong in an options collection. Do not expose every SDK setting by default.

### Opt-in batching and execution-error policy (0.1.2)

`options.batchSize` is a positive safe integer, default `1`. The default preserves one message per execution and existing single-message concurrency. A value greater than one collects up to that many messages, emits them as separate items in **one** execution, and waits for that execution and its settlements before starting another batch. There is no concurrent second batch, even when unused message capacity remains.

`options.maxInFlight` remains a finite **message** budget, not a batch count. `batchSize` must fit that budget. Collection uses the existing bounded `pullTimeoutMs`: an incomplete nonempty batch is emitted when the pull expires; an empty pull does not emit an execution or stop a manual listener. Progress starts as each message is fetched, including time spent collecting the rest of the batch. Preserve raw Text, Binary, per-message metadata, received order and item pairing. Do not split, merge or reinterpret application payloads.

`options.onExecutionError` is `redeliver` by default, preserving existing failed-execution behavior. Optional `acknowledge` treats an evidenced, normally completed node failure as terminal for transport; it does **not** turn the failed n8n run into success. A resolved promise or `status=error` alone is insufficient: require a valid terminal timestamp and public node-failure evidence. Full task data requires matching top-level/last-failed-task errors. A queue summary with empty task data requires a nonempty job ID, valid start time and public `error.node.name` matching the last executed node; it must not override contradictory full-task evidence. Job ID and last node alone are insufficient and this recognition is not a universal persistence proof. Rejected admission, persistence/infrastructure errors without that evidence, missing/unknown results, cancellation, crash and unfinished shutdown remain recoverable. Qualification must exercise normal node failure versus admission and persistence faults in the actual target host; no private-core, database or API dependency belongs in the runtime library.

All items share one execution outcome. Confirm eligible message ACKs individually; a batch is not an atomic broker transaction. An uncertain ACK is not NAKed, and does not prevent attempting the other eligible ACKs. Conversion/admission failures never qualify for terminal-error ACK. The application owns native n8n node retries and per-item continuation so one failed item need not prevent processing the rest. Do not add a custom retry engine, per-item outcome protocol, manual ACK node, HTTP/business-error classifier or listener-stop-on-business-error policy.

Received batch order does not guarantee strict FIFO across delayed redelivery, restart or overlapping subscribers. The broker can deliver fresh messages before a NAKed batch returns, and overlapping subscribers can receive other batches. No router, partitioning service, additional stream, KV, checkpoint store or database/CDC change is part of this extension.

## Acknowledgement and n8n Wait contract

The approved completion boundary is **successful execution or persisted Wait handoff**. It is not merely receiving a message, calling `emit()`, or enqueuing an n8n execution.

For each execution, create an n8n deferred completion promise and emit its message item or batch items with `ITriggerFunctions.emit(data, undefined, donePromise)`. Inspect the resolved `IRun`, including status and error state. Promise resolution by itself is not success.

| Observed outcome | Message treatment |
| --- | --- |
| `success`, without an execution error | Confirmed ACK. |
| `waiting`, without an execution error, after n8n persists the resumable execution | Confirmed ACK: ownership transfers to n8n. |
| Evidenced, completed node failure | Default: delayed NAK when connected. Optional `onExecutionError=acknowledge`: confirmed ACK, retaining the failed n8n run. |
| Failure to start/enqueue or unrecognized execution/persistence failure | Delayed NAK when connected; otherwise leave unacknowledged. |
| Canceled, crashed, rejected promise, absent result, or unknown state | Never ACK as success. Release for redelivery. |
| ACK confirmation timeout/disconnect | Report an unconfirmed outcome; do not claim the message was definitely ACKed or definitely not ACKed. |
| Failure after an acknowledged Wait handoff | Handled through n8n's resumed execution/error handling; no NATS redelivery is requested by this trigger. |

The n8n 2.42.3 trigger completion bridge can resolve for a waiting execution. The library must use that native lifecycle correctly, not keep a promise in memory and pretend it survives a restart. Verify persistence-before-ACK with actual Wait/resume and persistence-failure tests in both regular and queue mode. If the host cannot provide this boundary reliably, report the compatibility limitation instead of introducing private-core patches or silently ACKing earlier.

Keep delivery progress alive while waiting for n8n admission, execution, and ACK confirmation, using the SDK's `working()` mechanism at a cadence below the effective acknowledgement timeout. Account for a consumer's `BackOff` configuration when deriving that timeout. Stop progress timers when a delivery is settled or released.

In the inspected SDK, confirmed acknowledgement is `JsMsg.ackAck({ timeout })`; plain `ack()` does not confirm the server received the ACK. Use the official SDK, including delayed `nak()` and progress handling, rather than constructing protocol messages manually.

Malformed payloads are visible failures, not successful deliveries: record the failure using the public n8n trigger failure-recording mechanism and use the same redelivery policy. Do not silently ACK, terminate, or redirect poison messages. `MaxDeliver` and broker advisories remain operator-controlled; reaching the limit does not automatically create a dead-letter queue.

n8n `Continue On Fail`, an intentionally handled error, or a detached subworkflow can make the parent execution succeed. The trigger uses the parent's reported outcome, not an inferred outcome of every downstream service or independently running child workflow.

## Backpressure, lifecycle, and scaling

The local in-flight limit includes fetched messages, locally buffered messages, queued n8n executions, running executions, and unconfirmed settlements. Pull only when capacity exists. Avoid hidden SDK prefetch beyond that capacity and unbounded promise arrays.

Maintain one connection per active trigger, using the SDK's reconnect behavior and status notifications. No module-global connection shared between unrelated credentials. A failed/unauthorized connection must be observable, not an idle healthy-looking trigger.

On close, stop issuing pulls first, then allow in-flight deliveries to reach their completion boundary within the configured grace period. Never ACK unfinished work just to shut down. Close iterators, timers, and the connection; leave remaining deliveries eligible for redelivery. Do not delete the durable consumer.

Several processes may share a durable pull consumer. This distributes deliveries but does not eliminate redelivery after a crash, network partition, expired `AckWait`, or an uncertain ACK. Reconnect and rolling overlap must not create new consumers or reset a cursor. Ordering of business side effects is not guaranteed when more than one message is in flight.

Manual trigger testing receives real broker data through the selected consumer. It must be explicit that testing a production durable consumes its workload and can advance its ACK state. Do not manufacture a test message, create an implicit preview consumer, or silently disable acknowledgement. Stop the manual listener through n8n's documented trigger lifecycle.

On the validated n8n 2.42.3 host, manual close starts immediately after trigger output, before the completion callback. Stop pulls and close the iterator immediately, return the public close promise without waiting for execution completion, and retain only the already-emitted delivery for bounded settlement. The existing `shutdownGraceMs` option is therefore a configurable **manual delivery-settlement deadline**, default **30,000 ms from trigger output**. Success or persisted Wait within that period permits confirmed ACK. Cancellation, failed persistence or deadline expiry releases without success ACK and cleans up the connection and progress timers.

Expiry does not cancel the original downstream execution: it may continue and later succeed while its released message is redelivered, allowing duplicate effects. A later result must not re-acquire or acknowledge that released delivery. This manual-mode limitation is explicitly approved. Automatic executions do not acquire this runtime bound; their grace period starts only on actual trigger shutdown. Use the existing option, not an additional runtime control, and preserve the success/persisted-Wait acknowledgement boundary.

## Trigger output

One message produces one n8n item and one triggered execution. Use an envelope so headers and metadata cannot collide with payload fields:

```json
{
  "payload": { "orderId": "123" },
  "metadata": {
    "subject": "orders.created",
    "stream": "ORDERS",
    "consumer": "order-handler",
    "streamSequence": "123",
    "consumerSequence": "9",
    "deliveryCount": 1,
    "redelivered": false,
    "timestamp": "2026-10-09T12:00:00.000Z",
    "messageId": "order-created-123",
    "headers": { "Content-Type": ["application/json"] }
  }
}
```

JSON mode accepts objects, arrays, and JSON scalar values inside `payload`. Text mode returns UTF-8 text inside `payload`. Binary mode uses n8n's `binary.data` through `prepareBinaryData`, with metadata in `json`; it does not put bytes or base64 in `json.payload`.

Headers preserve repeated values. Absent message IDs are `null`. Sequences are decimal strings in output; reject unsafe SDK numeric sequence values rather than stringify an already rounded number. JSON payloads follow normal JavaScript JSON number semantics; users needing byte-level or numeric-representation fidelity can select Text or Binary. Do not add an application-specific JSON parser.

No connection handles, ACK callbacks, credential data, or private seeds may appear in items. Progress/ACK state is trigger-local; no manual ACK node is required for this first version.

## Publisher contract

Required: credential, literal subject, and payload format (`JSON`, `Text`, or `Binary`). Support expression-based subject and payload values per input item. Publishing subjects cannot contain subscription wildcards.

Fixed JSON editor text is parsed as JSON. Expressions provide already evaluated values: preserve their string, number, boolean, null, object or array type without parsing expression-result strings again. For example, `={{ "123" }}` publishes the JSON string `"123"`, not the number `123`; `={{ "hello" }}` is valid. Expressions returning serialized JSON text publish a string unless the caller explicitly parses it in the expression. Serialize the resulting JSON value exactly once. Text is encoded as UTF-8. Binary reads the selected input binary property using n8n's binary helper, not an assumed in-memory base64 layout.

Optional settings: headers, stable message ID, expected stream, and publish timeout. Preserve repeated headers. If a dedicated parameter conflicts with a reserved header such as `Nats-Msg-Id`, fail clearly instead of silently overriding either value.

Use `jetstream.publish()` and await its `PubAck`. Never fall back to Core NATS fire-and-forget publication. A duplicate acknowledgement is a successful publish result, not an error. Publish failures use normal n8n node errors and item pairing; respect explicitly selected n8n error-handling behavior.

Return one result per input item, linked with `pairedItem`:

```json
{
  "subject": "orders.created",
  "stream": "ORDERS",
  "sequence": "123",
  "duplicate": false,
  "messageId": "order-created-123"
}
```

The result describes broker storage, not consumer completion. A timeout can mean the broker stored the message but the response was lost; it is an ambiguous result, not proof of rejection. Use one connection for the node execution and always release it.

## Deduplication guarantees

A caller-supplied stable message ID maps to the SDK's `msgID` / `Nats-Msg-Id`. Retries of the same logical publication must reuse it. Do not automatically hash a payload or invent a new ID on every retry.

JetStream publication deduplication is bounded by the stream's configured duplicate window. Confirmed consumption ACKs reduce ACK-loss ambiguity but do not make database writes, HTTP calls, or n8n execution admission transactional with NATS.

The package therefore guarantees its stated acknowledgement boundary and **at-least-once delivery semantics**, not exactly-once external side effects. Applications needing exactly-once effects must provide idempotency at the effect boundary. The package does not mandate an idempotency database or a particular application pattern.

## Implementation and validation requirements

Keep TypeScript modules focused: credentials/connection, payload conversion, trigger delivery lifecycle, and publisher. Prefer the official SDK's existing features over new abstractions. Use descriptive names and English code, comments, UI labels, and documentation.

Provide build, lint, type-check, and test commands. Cover deterministic state/codec behavior with unit tests and broker behavior with a dedicated local/test JetStream instance. Tests must not use production streams or credentials. A container is an optional test harness, not a runtime requirement or justification for a custom n8n image.

Required observable acceptance cases:

1. Install the built package in the target n8n version; both nodes and credentials appear and load without private-core imports.
2. Credential modes and TLS success/failure behave as selected; credential testing does not mutate broker state.
3. Bind existing without creation permissions; create explicitly when absent; detect configuration mismatch and concurrent-create races without modifying an existing consumer.
4. One and multiple subject filters select only matching subjects; two clients sharing a durable distribute deliveries without independently resetting the cursor.
5. Successful processing gets a confirmed ACK; pre-boundary failure gets redelivery; no ACK occurs merely on `emit()` or queue admission.
6. A workflow longer than `AckWait` is kept alive while healthy. Queue backlog is included in the in-flight bound. Kill/disconnect the trigger and verify unacknowledged work is recoverable.
7. Wait state is persisted before ACK; restart and resume the same execution without requiring an in-memory trigger callback. A failure after the Wait follows n8n handling, not NATS redelivery. A persistence failure must not be treated as a durable handoff.
8. Regular mode, queue mode, manual execution, workflow deactivation, and overlapping trigger processes exercise the same stated completion contract. Record the tested host/server versions.
9. Publisher handles JSON/Text/Binary, repeated headers, item pairing, expected-stream rejection, duplicate IDs within a configured window, permission failures, and uncertain publish/ACK outcomes.
10. No timer/iterator/connection leaks after activation failure, repeated manual tests, reconnect, or shutdown. In-flight work and buffering stay within the configured bound.
11. Batch mode emits separate paired items in one execution, serializes batches, preserves raw Text and metadata, respects the message budget, heartbeats during collection and handles partial/empty pulls and manual shutdown.
12. Optional terminal-error ACK preserves the failed result; defaults redeliver. Actual target-host admission/persistence faults, cancellation and unknown results must not receive terminal-error ACKs. Native n8n RetryOnFail can succeed within one execution without broker redelivery.

Document actual measurements and remaining limitations. Do not mark a scenario as tested when only its mocked unit test passed. Do not publish to npm, deploy a runtime, change broker configuration, or modify another repository as part of implementation without separate authorization.

## References

- [n8n programmatic community-node tutorial](https://docs.n8n.io/connect/create-nodes/build-your-node/tutorial-build-a-programmatic-style-node.md).
- [n8n credentials contract](https://docs.n8n.io/connect/create-nodes/build-your-node/reference/credentials-files.md).
- [Official community-node peer-dependency rule, n8n 2.42.3](https://github.com/n8n-io/n8n/blob/n8n%402.42.3/packages/%40n8n/eslint-plugin-community-nodes/docs/rules/valid-peer-dependencies.md).
- [n8n trigger completion bridge, n8n 2.42.3](https://github.com/n8n-io/n8n/blob/n8n%402.42.3/packages/cli/src/workflows/triggers/trigger-execution-context.factory.ts).
- [n8n execution completion and Wait state, n8n 2.42.3](https://github.com/n8n-io/n8n/blob/n8n%402.42.3/packages/cli/src/active-executions.ts).
- [n8n execution persistence hooks, n8n 2.42.3](https://github.com/n8n-io/n8n/blob/n8n%402.42.3/packages/cli/src/execution-lifecycle/execution-lifecycle-hooks.ts).
- [NATS native authentication](https://docs.nats.io/learn/security/authentication-basics).
- [NATS decentralized JWT/NKey authentication](https://docs.nats.io/learn/security/decentralized-auth).
- [NATS TLS](https://docs.nats.io/learn/security/encryption).
- [JetStream publishing and deduplication](https://docs.nats.io/learn/jetstream/publishing).
- [JetStream acknowledgements and redelivery](https://docs.nats.io/learn/jetstream/acknowledgment).
- [JetStream subject filtering](https://docs.nats.io/learn/jetstream/filtering).
- [JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers).
- [Inspected official NATS SDK authenticators](https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/core/src/authenticator.ts).
- [Inspected SDK confirmed ACK implementation](https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/jetstream/src/jsmsg.ts).
- [Inspected SDK consumer creation implementation](https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/jetstream/src/jsmconsumer_api.ts).
