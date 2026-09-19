# Glossary

Terms are grouped in five sections and sorted alphabetically inside each. Every heading is a stable
anchor: link to it as `glossary.md#term-name` (lowercase, spaces become hyphens, punctuation dropped).
Each entry says which chapter introduces it and, where useful, where it lives in the repo.

- [Java and build](#java-and-build)
- [Kafka and CDC](#kafka-and-cdc)
- [Flink](#flink)
- [OCPP](#ocpp)
- [chargemon domain](#chargemon-domain)

---

## Java and build

### convention plugin
A Gradle plugin written in `buildSrc` that applies a bundle of settings to any module that names it (`chargemon.java-library`, `chargemon.flink-app`, `chargemon.spring-app`). Chapter 03. See [buildSrc/src/main/kotlin](../buildSrc/src/main/kotlin).

### Flyway
Database migration tool. Runs numbered SQL files (`V001__stations.sql` …) exactly once, in order, and records them in a history table. Chapter 03. See [schema/src/main/resources/db/migration](../schema/src/main/resources/db/migration).

### Optional
A Java container that holds either one value or nothing. Used instead of `null` for "may be absent" results. Chapter 02.

### pattern matching
Java 21 `switch` that branches on the runtime type of a sealed interface and unpacks record fields. The compiler checks that every permitted subtype is handled. Chapter 02.

### record
A Java class declared with `record Name(fields…)`: immutable, with constructor, accessors, `equals`, `hashCode` and `toString` generated. Most data types in this repo are records. Chapter 02. See [EventMeta](../ocpp-model/src/main/java/com/chargemon/ocpp/model/EventMeta.java).

### Result
This repo's own "either" type: `Result.Ok(value)` or `Result.Err(error)`. Used on hot paths so parsing failures do not throw exceptions. Chapter 02. See [Result.java](../common/src/main/java/com/chargemon/common/result/Result.java).

### sealed interface
An interface that lists exactly which types may implement it (`permits`). Lets the compiler prove a `switch` is exhaustive. Chapter 02. See [OcppEvent](../ocpp-model/src/main/java/com/chargemon/ocpp/model/OcppEvent.java).

### ServiceLoader
JDK plugin mechanism. A text file under `META-INF/services/<interface>` lists implementation class names; `ServiceLoader.load(Interface.class)` instantiates them. Used for OCPP mappers, condition operators and rule evaluators. Chapter 02.

### shadow jar
A single "fat" jar that bundles a module and all its dependencies (`*-all.jar`). Built by the Shadow plugin; `mergeServiceFiles()` keeps ServiceLoader registrations from different jars intact. Chapter 03.

### testFixtures
A Gradle source set (`src/testFixtures`) whose classes are shared with the tests of other modules. Example: [Frames.java](../ocpp-codec/src/testFixtures/java/com/chargemon/ocpp/codec/fixtures/Frames.java). Chapter 03.

### version catalog
`gradle/libs.versions.toml`: one file holding every dependency version, referenced as `libs.flink.core` in build scripts. Chapter 03.

---

## Kafka and CDC

### at-least-once
Delivery guarantee: every message arrives, but may arrive more than once after a failure. The consumer must be idempotent. chargemon's `alerts` topic is at-least-once. Chapter 04.

### compacted topic
A Kafka topic that keeps only the latest record per key (older values for the same key are removed). Behaves like a changelog of a table. Used for `stations`, `groups`, `rules`. Chapter 04.

### consumer group
A named set of consumers that share a topic's partitions between them; each partition is read by one member. Chapter 04.

### CDC
Change Data Capture: turning database row changes into a stream of events by reading the database's write-ahead log. Chapter 04.

### dead-letter topic
A topic where records that cannot be processed are parked for inspection instead of blocking the pipeline. chargemon has `dead-letter` (undecodable frames) and `alerts-dlq` (notifier retries exhausted). Chapter 04.

### Debezium
A CDC connector framework for Kafka Connect. chargemon uses its Postgres connector to stream the `rules` table into the `rules` topic. Chapter 04. See [rules-connector.json](../deploy/debezium/rules-connector.json).

### exactly-once
Delivery guarantee where each message's effect is applied precisely once. Expensive end to end; chargemon has exactly-once *state* inside Flink and at-least-once *output*. Chapter 04, 09.

### message key
Bytes attached to a Kafka record that decide its partition. Same key means same partition means preserved order. chargemon keys OCPP frames by station id. Chapter 04.

### offset
Position of a record inside a partition. Consumers commit offsets to remember where they are. Chapter 04.

### partition
An ordered, append-only log inside a topic. Ordering is guaranteed only within one partition. Chapter 04.

### pgoutput
Postgres's built-in logical-replication output plugin, used by Debezium to read row changes without extra extensions. Chapter 04.

### retry topic
Spring Kafka pattern: a failed record is republished to `<topic>-retry` with a delay instead of blocking the main consumer. Chapter 19.

### SMT
Single Message Transform: a Kafka Connect step that reshapes each record. chargemon uses `ExtractNewRecordState`, `ExtractField$Key` and `RegexRouter`. Chapter 04.

### tombstone
A Kafka record with a key and a `null` value. On a compacted topic it means "delete this key". Chapter 04.

### topic
A named stream of records in Kafka, split into partitions. Chapter 04.

---

## Flink

### broadcast state
State that is replicated to every parallel instance of an operator, fed by a `BroadcastStream`. Used to push rules and group records to all tasks. Chapter 08.

### checkpoint
A consistent snapshot of all operator state plus source offsets, taken periodically. On failure the job restarts from the last checkpoint. Chapter 05, 07.

### DataStream
Flink's typed, unbounded stream abstraction. Transformations on it build a graph lazily; nothing runs until `env.execute()`. Chapter 06.

### event time
The time embedded in the event itself (when it happened), as opposed to when Flink saw it. Chapter 05.

### job graph
The compiled DAG of operators Flink will run. `ProductionTopologyTest` builds it without executing. Chapter 06.

### JobManager
The coordinating process of a Flink cluster: schedules tasks, triggers checkpoints, serves the web UI on port 8081. Chapter 06.

### keyBy
Partitions a stream by a key so that all events with the same key go to the same parallel task and share keyed state. Chapter 06.

### keyed state
State scoped to the current key: `ValueState`, `MapState`, `ListState`. Each key sees only its own values. Chapter 07.

### KeyedBroadcastProcessFunction
A two-input function: one keyed stream and one broadcast stream. `processElement` reads broadcast state; `processBroadcastElement` may write it. Chapter 08.

### KeyedProcessFunction
The low-level keyed operator: `processElement` per event, keyed state, and timers with `onTimer`. Chapter 06, 07.

### MiniCluster
An in-process Flink cluster used by tests (`MiniClusterWithClientResource`). Chapter 06.

### operator chaining
Flink fuses consecutive operators with the same parallelism into one task to avoid serialization between them. `decode` is chained to the Kafka source on purpose. Chapter 06.

### operator uid
A stable identifier set with `.uid("…")`. Savepoints map state to operators by uid; changing it loses state. Chapter 06.

### parallelism
Number of parallel instances of an operator. Configured per job (`PARALLELISM`) and per operator. Chapter 06.

### processing time
The wall clock of the machine running the operator. Simple, but restarts and lag shift it. Chapter 05.

### ProcessFunction
The non-keyed low-level operator; can emit to side outputs but has no keyed state or timers. Chapter 06.

### RocksDB
An embedded key-value store used as a Flink state backend when state is larger than memory. Chapter 07.

### savepoint
A manually triggered, portable checkpoint used for upgrades and rescaling. Chapter 07, 09.

### side output
A secondary output stream of an operator, addressed by an `OutputTag`. Used for dead letters, late sessions and group membership deltas. Chapter 06.

### slot
A unit of resource in a TaskManager; one parallel task instance runs per slot. Chapter 06.

### state backend
Where keyed state lives: HashMap (heap) or RocksDB (disk). Chapter 07.

### state TTL
Time-to-live: keyed state entries expire after a period of no writes. Chapter 07.

### TaskManager
Worker process that runs operator tasks in its slots. Chapter 06.

### timer
A callback (`onTimer`) scheduled for a future processing time or event time, per key. Grace windows, correlation timeouts and absence detection are all timers. Chapter 07.

### TypeInformation
Flink's description of a type, used to pick a serializer. chargemon's `JsonTypes.of(Class)` supplies a Jackson-based one for every record. Chapter 07.

### TypeSerializer
Converts objects to bytes for state and network shuffles. Paired with a `TypeSerializerSnapshot` for schema evolution. Chapter 07.

### watermark
A marker flowing through the stream that says "no event older than time T will arrive". Drives event-time timers and window closing. Chapter 05, 09.

### window
A finite slice of a stream (tumbling, sliding, session) over which to aggregate. chargemon uses hand-rolled hourly buckets instead of Flink windows. Chapter 05.

---

## OCPP

### action
The name of an OCPP operation carried in a CALL, for example `BootNotification` or `StatusNotification`. Chapter 10.

### Authorize
Station asks the CSMS whether an idTag / idToken may charge. The answer is in the CALLRESULT. Chapter 10.

### BootNotification
First message a station sends after start-up; the CSMS answers Accepted / Pending / Rejected plus a heartbeat interval. Chapter 10.

### CALL
OCPP-J request frame: `[2, "<id>", "<Action>", {payload}]`. Chapter 10.

### CALLERROR
OCPP-J error frame: `[4, "<id>", "<ErrorCode>", "<description>", {details}]`. Chapter 10.

### CALLRESULT
OCPP-J response frame: `[3, "<id>", {payload}]`. Chapter 10.

### charge point
OCPP 1.6 name for the physical charging unit. OCPP 2.0.1 says "charging station". Chapter 10.

### connector
The physical socket or cable on a station. In 1.6 it is numbered per station; in 2.0.1 it is numbered per EVSE. Chapter 10.

### connector status
The state of a connector: Available, Preparing, Charging, Faulted … (1.6) or Available, Occupied, Reserved, Unavailable, Faulted (2.0.1). Chapter 10.

### CSMS
Charging Station Management System: the backend the stations talk to. OCPP 1.6 calls it the central system. Chapter 10.

### EVSE
Electric Vehicle Supply Equipment: in 2.0.1, one power outlet unit inside a station, which owns one or more connectors. Chapter 10.

### Heartbeat
Periodic keep-alive CALL from station to CSMS. Its absence is the classic "station offline" signal. Chapter 10.

### idTag / idToken
Identifier of the user or card starting a charge. `idTag` in 1.6, `idToken` with a type in 2.0.1. Chapter 10.

### measurand
What a meter sample measures, for example `Energy.Active.Import.Register` (the default) or `Power.Active.Import`. Chapter 10.

### message id
The unique id string in position 1 of every OCPP-J frame; matches a CALL with its CALLRESULT or CALLERROR. Chapter 10.

### MeterValues
Periodic meter samples sent during a transaction. Chapter 10.

### OCPP-J
The JSON-over-WebSocket binding of OCPP, defining the array framing used here. Chapter 10.

### registration status
Result of a BootNotification: Accepted, Pending or Rejected. Chapter 10.

### StatusNotification
Station reports a connector's status change. 1.6 also carries an error code. Chapter 10.

### transaction
One charging session, from start to stop. 1.6 uses StartTransaction / StopTransaction; 2.0.1 uses TransactionEvent with Started / Updated / Ended. Chapter 10.

### TransactionEvent
OCPP 2.0.1 message covering the whole session life: `Started`, `Updated`, `Ended`, each with meter values. Chapter 10.

---

## chargemon domain

### age-out timer
Timer set by the group-aggregate evaluator at "oldest counted alert + window + 1 ms" so the count drops when old alerts leave the window even if no new alert arrives. Chapter 15.

### aggregate snapshot
A record saying "subject X has N zero-energy sessions in window W as of time T". Produced by the zero-energy aggregator, written to Postgres and fed into rules as `agg.zeroEnergy.<window>`. Chapter 17. See [AggregateSnapshot](../flink-processor/src/main/java/com/chargemon/flink/model/AggregateSnapshot.java).

### alert key
`ruleId|subjectType|subjectId`. Identifies one alert lifecycle; the lifecycle operator is keyed by it. Chapter 16, 18. See [AlertKey](../flink-processor/src/main/java/com/chargemon/flink/model/AlertKey.java).

### auto-resolve
Optional rule timing: an OPEN alert is force-resolved after this duration even if the condition never clears. Chapter 16.

### canonical event
An `OcppEvent` record that means the same thing regardless of OCPP version. Rules only ever see canonical events. Chapter 11.

### ChannelRef
`type:target` string such as `slack:#ops` or `webhook:https://…`. Rules carry a list; the notifier routes by the type prefix. Chapter 16, 19. See [ChannelRef](../alert-model/src/main/java/com/chargemon/alert/ChannelRef.java).

### condition AST
The JSON tree of operators (`and`, `eq`, `gt`, `matches` …) that a rule evaluates against a Fact. Chapter 14.

### ConditionSignal
Output of a rule evaluator: TRIGGERED or CLEARED for a (rule, subject). Input to the alert lifecycle. Chapter 15. See [ConditionSignal](../alert-model/src/main/java/com/chargemon/alert/ConditionSignal.java).

### correlation
Joining a CALL with its later CALLRESULT or CALLERROR by message id, to produce events like `BootCompleted` that need both halves. Chapter 12.

### delivery ledger
Postgres table `notification_deliveries` keyed by (alert event id, channel). The notifier claims a row before sending, so a redelivered Kafka record is sent once. Chapter 19.

### edge-triggered
An EVENT rule emits TRIGGERED only when its condition flips from false to true, not on every event where it is true. Repeated matching events are absorbed. Chapter 15.

### envelope
The JSON record on the `common-broker` topic: station id, OCPP version, direction, receive time and the raw frame. Chapter 12. See [RawEnvelope](../ocpp-codec/src/main/java/com/chargemon/ocpp/codec/envelope/RawEnvelope.java).

### Fact
The rule engine's read-only view of an event plus context, accessed by dotted path (`event.status`, `station.vendor`, `agg.zeroEnergy.daily`, `now`). Chapter 12, 14. See [Fact](../rule-engine/src/main/java/com/chargemon/rules/condition/Fact.java).

### field path
A dotted path into a Fact, with optional array index, e.g. `event.meterValues[0].sampledValues[0].value`. Chapter 12.

### frame
The OCPP-J array (`[2, …]`, `[3, …]`, `[4, …]`) inside an envelope, parsed into `RawFrame.Call / CallResult / CallError`. Chapter 12.

### GenericOcppEvent
Fallback canonical event for any action without a dedicated mapper; keeps the payload as JSON text reachable via `event.payload.*`. Chapter 11.

### grace window
Rule timing: a TRIGGERED condition must stay true this long before the alert opens. Clears inside the window cancel silently. Chapter 16.

### hourly bucket
One counter per epoch hour per subject. Every window (hourly, daily, rolling 7d/30d) is derived by summing buckets. Chapter 15. See [HourlyBucketAggregator](../rule-engine/src/main/java/com/chargemon/rules/window/HourlyBucketAggregator.java).

### lifecycle phase
State of one alert key: IDLE, PENDING, OPEN, RESOLVED, SUPPRESSED. Chapter 16. See [Phase](../rule-engine/src/main/java/com/chargemon/rules/lifecycle/Phase.java).

### mapper
A class that turns one (version, action) payload into a canonical event. Registered via ServiceLoader. Chapter 12. See [OcppActionMapper](../ocpp-codec/src/main/java/com/chargemon/ocpp/codec/mapper/OcppActionMapper.java).

### member delta
ADD / REMOVE of a station in a group, announced by the enrichment operator so group rules know how many members exist. Chapter 17. See [GroupMemberDelta](../flink-processor/src/main/java/com/chargemon/flink/model/GroupMemberDelta.java).

### parked response
A CALLRESULT that arrived before its CALL (different Kafka partitions). Held until the CALL shows up or the correlation timeout fires. Chapter 12.

### rule kind
EVENT, ABSENCE, STATE_DURATION (stage 1, per station) or SEQUENCE, GROUP_AGGREGATE (stage 2, over stage-1 alerts). Chapter 14.

### stage 1 / stage 2
Stage 1 rules evaluate station events. Stage 2 rules evaluate stage-1 alerts (composites per station, aggregates per group). Only one level of nesting. Chapter 13.

### stale timer
A timer that fires after the deadline it was set for has been replaced or cleared. Lifecycle and rule operators compare the firing time with the stored deadline and ignore it on mismatch. Chapter 16, 18.

### station filter
Optional condition on `station.*` in a rule, restricting it to stations by vendor, model or attribute. Chapter 14.

### subject
What an alert is about: a STATION or a GROUP, identified by `SubjectRef`. Chapter 13. See [SubjectRef](../alert-model/src/main/java/com/chargemon/alert/SubjectRef.java).

### suppression window
Rule timing: after an alert opens, re-triggers on the same subject are muted for this long. Chapter 16.

### target groups
Optional list of group ids on a rule; the rule applies only to stations under those groups (transitively). Chapter 14.

### zero-energy session
A charging transaction that ended with exactly 0 Wh delivered. Counted per station and per ancestor group. Chapter 17. See [SessionTracker](../flink-processor/src/main/java/com/chargemon/flink/energy/SessionTracker.java).
