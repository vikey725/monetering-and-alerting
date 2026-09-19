# chargemon — OCPP monitoring & alerting

Stream processor for ~1M EV charging stations speaking OCPP 1.6 and 2.0.1. It normalises raw
OCPP frames from Kafka into one canonical event model, evaluates dynamic alert rules, manages
alert lifecycle (grace / suppression windows), tracks zero-energy sessions with rolling
aggregates, and hands alerts to a notifier (Slack, PagerDuty, email, webhook).

```
common-broker ─► decode ─► correlate ─► enrich ─┬─► stage-1 rules ─► lifecycle ─► alerts ─┬─► Kafka `alerts` ─► notifier
 (Kafka)                    (CALL⇄RESULT)  (station+groups) │      (EVENT/ABSENCE/STATE_DURATION)   │              └─► Postgres `alerts`
                                                            │                                        └─► stage-2 rules (SEQUENCE / GROUP_AGGREGATE) ─► lifecycle ─┘
                                                            └─► sessions ─► zero-energy hourly buckets ─► snapshots (station+group) ─► Postgres, rules
stations / groups (compacted topics) ─► enrich + Postgres mirror
rules (Postgres ─► Debezium ─► compacted topic) ─► broadcast to every rule operator (hot reload)
```

## Learning guide

New to Flink, OCPP or this codebase? Start with the chapter-wise guide in [`docs/README.md`](docs/README.md).

## Modules

| Module | Purpose |
|---|---|
| `common` | Jackson factory, durations, UUIDv7, `Result`, `Env` |
| `ocpp-model` | Canonical sealed event hierarchy (`OcppEvent`), session id, station/group records |
| `ocpp-codec` | Envelope/frame parsing, `(version, action)` mapper registry (ServiceLoader), CALL/RESULT correlator, `Fact` bridge |
| `alert-model` | `AlertEvent`, `ConditionSignal`, `ChannelRef`, `RuleTiming` — the contract with downstream |
| `rule-engine` | JSON condition AST + operators, rule definitions/validator, kind evaluators, `AlertLifecycle` state machine, hourly-bucket window aggregator. Pure Java, no Flink |
| `schema` | Flyway migrations (`stations`, `station_groups`, `rules`, `alerts`, `zero_energy_*`, `notification_deliveries`) |
| `flink-processor` | The Flink 1.20 job: operators are thin adapters over rule-engine + codec |
| `notifier` | Spring Boot consumer of `alerts` with per-channel delivery, idempotency ledger, retries/DLQ |
| `event-generator` | CLI producing realistic OCPP traffic with fault profiles |
| `deploy` | docker-compose stack, Dockerfiles, Debezium connector, k8s `FlinkDeployment` |

Dependency direction: `common ← ocpp-model ← ocpp-codec`, `common ← alert-model ← rule-engine ← ocpp-codec`,
everything else depends on those. `rule-engine` never sees OCPP types: it evaluates a `Fact`.

## Extending

| Want to… | Do |
|---|---|
| Support a new OCPP action | Add an `OcppActionMapper` (or `CorrelatedMapper`) in `ocpp-codec` + one line in `META-INF/services`. Until then the action is still rule-evaluable as `GenericOcppEvent` via `event.payload.*` |
| Support a new OCPP version | New `OcppVersion` constant + a `v2xx` mapper package |
| Add a condition operator | Implement `Condition`, register it in a `ConditionOperatorProvider` |
| Add a rule kind | New `RuleSpec` record + `StationRuleKindEvaluator` / stage-2 evaluator + services line; parser switch case |
| Add a notification channel | Implement `NotificationChannel` as a Spring bean; `type()` matches the channel ref prefix |
| Add an aggregate source | Emit `AggregateSnapshot` with a new `source`; rules read it as `agg.<source>.<window>` |

## Rules

Rows in `rules` (Postgres) flow through Debezium to the compacted `rules` topic and are
broadcast to the job; edits apply within seconds, no restart. Every rule has:

- `graceWindow` — condition must stay true this long before an alert opens (ISO-8601, e.g. `PT2M`)
- `suppressionWindow` — after opening, re-triggers on the same subject are muted this long
- `autoResolveAfter` — optional forced resolve
- `targetGroupIds` — restrict to stations under these groups (transitive); empty = all
- `stationFilter` — optional condition on `station.*`
- `severity`, `channels` (`slack:#ops`, `pagerduty:svc`, `email:a@b`, `webhook:https://…`)

Kinds and specs (see `deploy/local/sample-rules.sql` for full rows):

```jsonc
EVENT           {"trigger":{"actions":["StatusNotification"]} | {"aggregate":"zeroEnergy"}, "condition":AST, "clear":AST|null}
ABSENCE         {"expectedAction":"Heartbeat","within":"PT10M","onlyIf":AST|null}          // "*" = any action
STATE_DURATION  {"enter":AST,"exit":AST,"maxDuration":"PT30M","scopeField":"event.connectorId"}
SEQUENCE        {"allOf":["ruleA","ruleB"],"within":"PT15M"}                                // stage-1 rule ids, same station
GROUP_AGGREGATE {"source":{"type":"alerts","ruleId":"…"} | {"type":"zeroEnergy","window":"rolling7d"},
                 "window":"PT1H","threshold":{"count":10} | {"percent":20}}                // subjectType GROUP
```

Condition AST: `and`, `or`, `not`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `nin`, `between`,
`matches`, `exists`, `startsWith`, `contains`. Fact paths: `event.*` (typed fields, `event.type` = action for mapped events or `Generic`, `event.action` = wire action,
`event.payload.*` for generic events), `station.*` (`vendor`, `model`, `attributes.x`, `allGroupIds`),
`agg.zeroEnergy.<window>` (`hourly`, `daily`, `rolling7d`, `rolling30d` by default), `now`.

Alert lifecycle per `(rule, subject)`: `IDLE → PENDING →(grace) OPEN →(cleared) RESOLVED →(suppression) IDLE`;
re-triggers during suppression become `SUPPRESSED` and open a fresh grace window when suppression ends.

## Running locally

```bash
export JAVA_HOME=~/.jdks/jdk-21.0.12.1+1     # any JDK 21
./gradlew build                              # unit + MiniCluster + Testcontainers tests
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U chargemon -d chargemon < deploy/local/sample-rules.sql
java -jar event-generator/build/libs/event-generator-all.jar \
     --stations 200 --rate 100 --profiles normal,heartbeat-drop,stuck-preparing,zero-energy,boot-rejected,call-error \
     --seed-master --speedup 10
```

Then: Flink UI http://localhost:8081, Kafka UI http://localhost:8090 (`alerts` topic),
Mailpit http://localhost:8025 (notifier fallback channel), Postgres tables `alerts`,
`zero_energy_rolling`, `notification_deliveries`.

Configuration is via environment variables (`KAFKA_BOOTSTRAP`, `TOPIC_*`, `DB_*`,
`CORRELATION_TIMEOUT`, `ZERO_ENERGY_WINDOWS`, `PARALLELISM`, …); see `JobConfig` and the
notifier's `application.yml`. Kubernetes: `deploy/k8s/flink-deployment.yaml` (Flink Kubernetes Operator,
application mode, RocksDB, S3 checkpoints).

## Delivery semantics

- Flink checkpoints give exactly-once state; the `alerts` Kafka sink is at-least-once (low latency).
  Duplicates are absorbed by the notifier ledger (`alertEventId × channel`) and by the Postgres upsert (`last_seq` guard).
- Rules are pre-loaded from the compacted topic when an operator starts and then updated by broadcast,
  so there is no window where events are evaluated without rules.
- The correlator parks responses that arrive before their CALL (different partitions) for the correlation timeout.

## Known limitations

- Station master data is joined from keyed state; on a *fresh* start (no checkpoint) events that arrive before
  the `stations` topic replay reaches the operator are enriched as "unknown station". Restarts from checkpoints are unaffected.
- Group membership counters (for percentage thresholds) are announced from each station's transitive closure and
  re-diffed on the station's next event, so late group records and re-parenting converge once stations send traffic.
- Stage-2 (composite/group) rules consume stage-1 alerts only — one level of nesting.
- Editing a rule's condition does not reset evaluator state for subjects already triggered under the old version.
