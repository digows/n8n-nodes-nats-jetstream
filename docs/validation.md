# Validation evidence

The manual grace-period limitation was explicitly approved and is recorded in the canonical [specification.md](specification.md). The historical implementation measurements below belong to commit `c4a86f41988baeea73ab7e57cdf407a850bdab23`, before the package and registration identifiers were renamed. They are not new-name installation measurements; rename verification and the subsequent public release are recorded separately.

## 0.1.2 candidate: batching and terminal execution-error policy

Measured on macOS arm64, Node **26.8.1**, npm **11.19.0**, official node CLI **0.51.4**, n8n-workflow development interface **2.42.2** and NATS SDK **3.4.0**. Native NATS **2.14.6** archive SHA-256 `291b9aa8c342b3cdcc53d872585bd467de4b5c9acf375066d882eb5412a09d55` and negative compatibility fixture **2.9.25** SHA-256 `fdec68b67230f0fcf8ffbf6d0a2e8ac1b4c6b8298ba64acd7000b5fe6212ccc1` matched their official release checksums. JWT fixture generator: **0.0.10-5**. All brokers and credentials were synthetic/disposable; fixtures honored task-specific `TMPDIR`.

- `npm run check`: build, official lint, typecheck and **30/30 unit tests**, zero failures/skips (unit portion 917.7 ms), including the reviewed queue-summary predicate.
- `npm run test:broker`, with `NATS_SERVER_BIN` pointing to 2.14.6 and the explicit old/JWT fixture paths: **24/24 tests**, zero failures/skips, **20.642 s**. This fresh run includes authentication/TLS, consumer creation, publisher, bounded single delivery, overlapping processes/crash, reconnect, old-server compatibility and uncertain ACK regressions.
- New broker cases verify one batch at a time even with excess capacity, raw Text/headers/metadata/pairing, progress through collection/execution, partial and empty pulls, terminal error ACK versus default/unknown-result redelivery, manual settlement, unfinished shutdown and Binary helpers.
- `node --check tests/host/wait.test.cjs` and `git diff --check`: passed. Actual n8n **2.42.6** CI has exercised the new host cases, but full acceptance of the queue-summary correction remains **pending**. It must pass both modes, including native Code errors, HTTP 401 after three native attempts, default redelivery, admission/save rejection, cancellation, Wait and manual batching. This candidate is not yet published.

The terminal-node predicate uses only public `IRun` fields: error status, finite terminal timestamp, last executed node, errored final task, and deep-equal task/top-level error when task data exists. An empty-task queue summary instead requires a nonempty job ID, valid start time and matching public `error.node.name`. It never overrides contradictory full-task data. Job ID/last node alone are insufficient; unknown or mismatched evidence remains recoverable, not acknowledged as a known terminal node failure.

[Instrumented diagnostic run 38990325764](https://github.com/digows/n8n-nodes-nats-jetstream/actions/runs/38990325764) captured the pre-fix public callback: regular mode supplied complete tasks and passed the predicate; queue mode supplied `runData: {}`, job ID and a node-labelled error, so the original predicate incorrectly requested redelivery. Temporary observation used only the package's public trigger context, not private n8n interfaces; that run is **not** unmodified-artifact acceptance. The probe/preload has been removed for the corrected candidate. Inspected source and this callback capture do not prove every possible persistence/preflight path: the native admission/save-fault tests and final unmodified host run remain mandatory.

### Synthetic backlog benchmark (not n8n throughput)

`NATS_SERVER_BIN=<2.14.6 binary> node tests/bench/batch.cjs` completed with 1,000 prepublished messages per scenario, fixed **2 ms per execution** in a public-interface adapter, zero duplicate/redelivered messages, and confirmed ACK completion. Publication is outside the clock; activation is inside. No n8n admission, persistence, application I/O or low-volume collection timeout is represented.

| Scenario | Message budget | Executions | Total through ACKs | Messages/s | Completion latency p50 / p95 / p99 |
| --- | ---: | ---: | ---: | ---: | --- |
| Serial single | 1 | 1,000 | 3,171.84 ms | 315.27 | 1,616.34 / 3,008.52 / 3,131.85 ms |
| Concurrent single | 50 | 1,000 | 101.65 ms | 9,838.15 | 49.85 / 91.48 / 92.81 ms |
| Serial batch of 50 | 50 | 20 | 196.82 ms | 5,080.74 | 87.39 / 172.53 / 180.83 ms |

Batching reduced execution count **50-fold** and beat serial single processing in this synthetic workload; concurrent single processing was faster than serialized batches. These are measured adapter results, not a production speedup or evidence that batching always improves throughput. Batch ACKs are confirmed individually and not atomically. Delayed NAK, restart and overlapping subscribers do not preserve strict FIFO.

## Public release and subsequent runtime checks

Version [0.1.0](https://www.npmjs.com/package/n8n-nodes-nats-jetstream/v/0.1.0) was published from tag `v0.1.0`, source commit `52f1c798bbe33ec6e612335be902f11f3cb86ac1`. [Publication run 38029187121](https://github.com/digows/n8n-nodes-nats-jetstream/actions/runs/38029187121) passed build/lint/types, 18 unit tests and 15 real-broker tests with zero failures/skips on Node 24.21.0 and npm 11.19.0. The public tarball's downloaded SHA-1 and SHA-512 matched the reviewed artifact. The registry exposes provenance attestations for this version.

Package registration, credential loading, startup and readiness were subsequently checked in a custom self-hosted n8n 2.42.6 image, including main and queue-worker processes. Those checks do not replace the functional n8n 2.42.3 host suite below or establish universal compatibility.

A separate native community-package installation attempt in that custom image returned `Cannot find module 'n8n-workflow'`. The successful image-bundled loading and the native installer failure are distinct paths; this observation does not establish a general failure on all n8n 2.42.6 installations. Native installer diagnosis is separate from the published artifact and the historical fresh-install host tests. Do not treat the standard installation instructions as evidence that every custom image layout has been validated.

## Measured environment

| Component | Measured version |
| --- | --- |
| Node.js | 24.21.0, macOS arm64 |
| n8n host | 2.42.3 |
| n8n-workflow development/host interface | 2.42.2 |
| Official n8n node CLI | 0.51.4 |
| @nats-io/transport-node, @nats-io/jetstream | 3.4.0 |
| Primary native nats-server | 2.15.1 |
| Negative creation/filter compatibility and existing binding fixture | nats-server 2.9.25 |
| Queue database | PostgreSQL 18.4 via embedded-postgres 18.4.0-beta.17 |
| Queue broker | Redis 8.10.0 |
| Official JWT fixture generator | @nats-io/jwt 0.0.10-5, test-only external fixture tool |

All server/host processes and databases were dedicated local fixtures. No n8n private-core imports or modifications are used by the package. Host source was inspected to diagnose public lifecycle ordering; host tests use fixture-only REST calls and database assertions/fault injection.

## Historical implementation measurements

`npm run check` passed build, official lint (zero rule errors/warnings), TypeScript checks and 8 deterministic unit tests. The CLI emits its own terminal color environment warnings; those are not lint findings.

With the explicit fixture paths described in README, `npm run test:broker` passed 15 tests, zero failures/skips:

- All native authentication modes; malformed/wrong credentials fail without anonymous fallback. Complete `.creds` works with or without its final newline.
- Verified custom-CA TLS and mTLS; untrusted CA or absent client certificate fails. No insecure verification option.
- Credential testing connects/flushes/closes without changing streams. Failed tests return Error.
- Existing-consumer binding with info/pull/ACK/inbox permissions and no creation/account-wide API access.
- Concurrent create-only activation, incompatible configuration preservation and incompatible-create race re-reading. The observed race response is API error 10148, status 400.
- Multiple filters select only matching subjects. On 2.9.25 multiple filters fail explicitly, leaving no broad consumer. An additional probe measured that 2.9.25 ignores create-only action and updates a conflicting consumer in a creation race. Creation now requires 2.10 or newer; the rerun verified that both single-/multiple-filter creation fail before broker mutation on 2.9.25 and existing binding remains non-mutating.
- JSON/Text/Binary publication, input pairing, repeated headers, stable-ID duplicate acknowledgement and expected-stream rejection.
- No ACK merely on adapter emission; two in-flight slots remain bounded while progress prevents redelivery beyond AckWait.
- Delayed NAK on failed adapter execution, shutdown release and real broker data in the manual adapter.
- Two separate trigger processes share one durable; killing one with SIGKILL redelivers its unacknowledged message to the other without resetting the cursor.
- Graceful broker restart reconnects using the same consumer/cursor. The observed SDK bounded-fetch shutdown error is `JetStreamStatusError: server shutdown`.
- Repeated manual-adapter/shutdown cycles and activation failure return monitored connection count to baseline.
- Broker permission failures, publication stored despite a lost reply, and visible unknown ACK confirmation when ACK permission is denied.

These broker tests invoke node methods with public-interface context adapters. They are not substitutes for actual n8n lifecycle tests.

Before adding the real manual acceptance case, the installed-package host suite passed both regular and queue tests: 2 passed, zero failures/skips. Both nodes and credentials loaded in n8n 2.42.3. It measured:

- Actual persisted Wait rows and resumable execution data corresponding to confirmed broker ACKs.
- Main-host restart followed by resumption of the same execution, then a forced downstream failure without NATS redelivery after handoff.
- SQLite/PostgreSQL fault injection rejecting the waiting-state update, followed by NATS redelivery rather than a successful handoff.
- Queue admission with no worker: local in-flight bound of 2, third message still pending, progress preventing redelivery; a worker then completes the durable handoffs.

The package was subsequently rebuilt, packed and reinstalled into the isolated host for the manual-close experiment below. The earlier manual failure and the candidate's passing measurements are recorded separately.

## Earlier manual failure and lifecycle correction

Before the bounded-close candidate, repeated real manual executions through `/rest/workflows/<id>/run` failed in both regular and queue mode at the expected waiting-row/ACK acceptance condition (0 passed, 2 failed, zero skips). Native lifecycle and persistence-failure assertions were ordered before that case and passed in both modes. Total measured host-suite duration was 93.3 seconds.

The measured behavior is explained by the installed n8n 2.42.3 source:

1. `n8n-core/dist/execution-engine/triggers-and-pollers.js` installs manual `emit()` after `trigger()` returns. The supplied `donePromise` resolves in a `workflowExecuteAfter` hook.
2. `n8n-core/dist/execution-engine/workflow-execute.js`, `processNodeOutput()`, invokes the trigger's `closeFunction` immediately after obtaining its output.
3. `processSuccessExecution()` awaits that close promise before running `workflowExecuteAfter`.
4. Waiting for the execution result inside close consequently blocks the host result until the shutdown grace expires. The delivery is then released, not confirmed as a successful Wait handoff.
5. The public `ITriggerFunctions` interface has no execution abort signal. The earlier conclusion that cancellation never runs the completion hook was incorrect: `setupCancellation()` sets canceled status, aborts and separately invokes `workflowExecuteAfter`; `processSuccessExecution()` skips it because cancellation already invoked it. The installed target source and the real cancellation tests below confirm this correction.

ACK-on-emit, private lifecycle hooks, removing close, and leaving indefinitely detached progress/connection tasks are not acceptable substitutes. The candidate below uses only the public close and completion interfaces and retains the approved ACK boundary.

## Bounded public manual-close implementation

Manual close stops pulls and closes the iterator immediately. Its public promise then returns without awaiting the host's completion promise. Only the already-emitted delivery settles asynchronously, bounded by the existing `shutdownGraceMs` option; regular/queue automatic close retains its previous behavior. Success or persisted Wait permits confirmed ACK; cancellation, rejected persistence or deadline expiry never becomes success ACK. Settlement or deadline closes the connection and stops progress. No private lifecycle hooks or new runtime option were added.

The expanded `npm run test:host` run passed 2/2 target-mode suites, zero failures/skips, in 44.346 seconds (regular 17.732 seconds; queue 26.504 seconds). Each suite measured:

- Actual persisted Wait, main restart, signed resumption of the same execution and post-handoff failure without requested NATS redelivery.
- Forced SQLite/PostgreSQL waiting-state persistence failure and actual broker redelivery.
- Two real manual Wait executions, confirmed ACKs and successful resumption; a subsequent message remained pending without a listener.
- Manual HTTP processing for 500 ms with AckWait 150 ms and grace 2,000 ms: no ACK while incomplete, no redelivery, then successful execution and confirmed ACK.
- REST cancellation before a message: canceled execution, no delivered message and connection cleanup. Cancellation during a 5,000 ms HTTP request: canceled execution, cleanup and recoverable redelivered message.
- Manual Wait persistence rejection: no waiting-state handoff, cleanup and actual recoverable redelivery, never success ACK.
- After each settlement, monitored connections returned to the single fixture-client baseline; broker inbound-message counters remained unchanged for 300 ms, demonstrating no subsequent progress/pull traffic from that trigger.
- Manual processing longer than grace: with grace 250 ms and HTTP processing 1,500 ms, cleanup plus a 300 ms inactivity assertion completed 564 ms after publication in regular mode and 556 ms in queue mode. The released message was recovered through a separate native consumer before the original n8n execution later reported success.

### Approved manual-mode limitation

On n8n 2.42.3, manual close starts immediately after trigger output. Consequently the existing grace option becomes a manual delivery-settlement deadline (default 30 seconds). It does **not** cancel downstream execution when it expires. A later successful execution does not re-acquire or ACK that released delivery, so duplicate business effects remain possible. Automatic execution does not gain this runtime limit; its grace starts on actual trigger shutdown.

These measured semantics were explicitly approved after their redelivery/duplicate-effect consequence was explained. The canonical specification and the existing option's UI description now state them. No unsupported-manual fallback or additional option was introduced.

## Historical finalization scope — c4a86f4

The broker result (15 passed, zero failures/skips) and expanded host result (2 mode suites passed, zero failures/skips) apply to the runtime and fixtures included in this implementation commit. After those measurements, finalization changed only the canonical manual-mode clarification, README/report wording and the existing option's descriptive UI text. No execution, settlement, connection, consumer, payload, publishing or fixture behavior changed. Final `npm run check` passed build, official lint with zero findings, typecheck and 8 unit tests. A direct comparison against the installed package used by the passing host suites confirmed identical generated connection/consumer/delivery/payload/credential/publisher modules and an identical trigger method; only descriptive UI text differs. Whitespace checks and `npm pack --dry-run --ignore-scripts --json` passed; the package contains 27 distribution/documentation files, without tests or temporary artifacts. Broker/host results are reused for unchanged behavior, not reported as a new run.

## Not claimed

Other n8n versions, Cloud verification, arbitrary broker versions, production performance, exactly-once external effects and automatic dead-letter handling are not established by these tests. The 2.9.25 fixture is not general compatibility coverage. The create-only action is present in the [nats-server 2.10.0 source](https://github.com/nats-io/nats-server/blob/v2.10.0/server/consumer.go); the version gate protects the approved no-overwrite contract rather than emulating it on older servers.

## Rename verification — n8n-nodes-nats-jetstream

The existing GitHub repository was renamed, not replaced: repository ID `1412479767` and node ID `R_kgDOVDC7Fw` remained unchanged, with default branch `main`. Its new full name is `digows/n8n-nodes-nats-jetstream`; the local clone and origin now use that name. The original commits and measurements above remain history.

Current registrations are `n8n-nodes-nats-jetstream.natsJetstream`, `n8n-nodes-nats-jetstream.natsJetstreamTrigger` and credential `natsJetstreamApi`, exported through `NatsJetstream`, `NatsJetstreamTrigger` and `NatsJetstreamApi`. Source comparison against `c4a86f4` confirmed that runtime changes are only identifier, path and display-name replacements; message, ACK, authentication and consumer behavior is unchanged.

New-name validation used Node 24.21.0, n8n 2.42.3, n8n-workflow 2.42.2, official SDK 3.4.0, nats-server 2.15.1/2.9.25, PostgreSQL 18.4 and Redis 8.10.0:

- `npm run check`: build, official lint with zero findings, typecheck and 8 unit tests passed, zero failures/skips.
- Package-entry/metadata assertions instantiated all three new classes, checked their registrations and lockfile name, and verified that generated files contained no old class paths. The previous generated `dist` was removed before rebuilding, avoiding stale old-name entries.
- `npm run test:broker`: 15 passed, zero failures/skips, 22.958 seconds, with new imports and trigger-process fixture identifiers.
- The built new-name tarball was installed into a fresh isolated community-node directory. `npm run test:host`: 2 mode suites passed, zero failures/skips, 60.354 seconds (regular 21.664 seconds; queue 38.582 seconds). Both new node registrations loaded, the new credential was created/used, and persisted Wait, restart/resume, forced persistence failure, repeated manual execution, cancellation, cleanup, queue backlog and manual deadline cases passed.
- The first new-name host run passed queue mode but hit SQLite `database is locked` while the test connection created its synthetic failure trigger in regular mode. The fixture now sets a bounded 5,000 ms SQLite busy timeout; the complete rerun above passed without changing package runtime or assertion scope.
- The fresh fixture's SQLite native dependency initially failed to build with its bundled node-gyp 8.4.1 and Python 3.14.8. Rebuilding that temporary dependency with node-gyp 11.5.0 succeeded under Node 24.21.0; no package/host source patch was made.

These are actual new-name integration/install measurements, not a relabeling of the historical results. The approved manual deadline semantics remain unchanged. No CI, npm publication or runtime deployment was added as part of the rename.

## Current-tree naming acceptance

Historical versions, timings and commit hashes remain unchanged, but current documentation no longer spells out obsolete package/class/registration names. This documentation-only follow-up does not change the runtime or the fixtures measured at `0edb91fe45a35abbb6a9ffd3a5dbbd6ce9e4dcbf`.

The case-insensitive content and path scans below must produce no matches (ripgrep exit code 1). Dependencies and immutable Git internals are excluded; generated distribution files and source maps are included even though gitignored. Repeat the scans inside the extracted installation tarball, where there are no dependencies or Git internals. Package-entry checks additionally verify the current package, lockfile, repository URLs, node and credential registrations.

```sh
rg --text --no-ignore --hidden -n -i \
  --glob '!node_modules/**' --glob '!.git/**' \
  'jetstream[ _-]?nats' .
find . -path './node_modules' -prune -o -path './.git' -prune -o -print \
  | rg -i 'jetstream[ _-]?nats'
```

## Publisher expression value correction

The publisher execution regression suite uses the exported `Workflow` and its real public expression evaluator from n8n-workflow 2.42.2. Only connection creation and the SDK publish boundary are mocked; expressions are not replaced with canned resolved values. On the previous runtime (`29c5b08e13b47146828905fedee1532f05e40b75`), `node --test tests/unit/publisher.test.cjs` passed 4 tests and failed 2 expression-string tests, including rejection of `hello` as invalid JSON. After the bounded correction, all 6 publisher tests pass.

The publisher reads `jsonPayload` through public `getNodeParameter(..., { rawExpressions: true })` only when the resolved JSON value is a string. It parses fixed JSON editor text, but preserves an expression-result string. There is no new setting, private core import or change to authentication, ACK, Wait or consumer behavior.

The corrected tree was measured locally with Node 24.21.0, npm 11.19.0, n8n-workflow 2.42.2, official node CLI 0.51.4 and NATS SDK 3.4.0:

- `npm run check`: official build/lint with zero findings, types and 18 unit tests passed, zero failures/skips.
- Publisher execution tests cover per-item string values including JSON-looking strings, direct string expressions, native number/boolean/null/object/array expressions, fixed JSON text, invalid fixed JSON, item pairing, connection closure, UTF-8 text and the binary helper. Captured publication bytes are compared exactly, detecting type coercion and double serialization.
- `npm run test:broker`: 15 passed, zero failures/skips, 17.588 seconds, using checksum-verified native NATS 2.15.1 and 2.9.25 and official JWT fixture 0.0.10-5. This is a fresh broker run for the correction, not a reused historical result.

The actual n8n regular/queue host suite was not rerun for this correction. These measurements do not establish behavior on the proposed n8n 2.42.6 image; its build, registration and devops rollout validation are a separate authorized follow-up. No image, manifest, release tag, npm publication or deployment was changed by this package correction.
