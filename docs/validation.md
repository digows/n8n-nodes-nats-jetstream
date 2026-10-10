# Validation evidence

The manual grace-period limitation was explicitly approved and is recorded in the canonical [specification.md](specification.md). No npm publication or production deployment has been performed. The historical implementation measurements below belong to commit `c4a86f41988baeea73ab7e57cdf407a850bdab23`, before the package and registration identifiers were renamed. They are not new-name installation measurements; rename verification is recorded separately.

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
