# Surge Kafka Streams Command Engine

The surge Kafka Streams command engine is a CQRS/ES engine
that uses Akka actors to represent aggregates in memory and
uses Kafka Streams as both the primary data store and the event
bus.

To use the library:

In sbt for scala:
```sbt
resolvers ++= Seq(
  "gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/"
)
libraryDependencies ++= Seq(
  "com.ultimatesoftware" %% "surge-engine-ks-command-scaladsl" % "0.1.0"
)
```

In sbt for java:
```sbt
resolvers ++= Seq(
  "gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/"
)
libraryDependencies ++= Seq(
  "com.ultimatesoftware" %% "surge-engine-ks-command-javadsl" % "0.1.0"
)
```

In maven for java:
```xml
<project>

    ...

    <repositories>

       ...

        <repository>
            <id>gears-tools-maven-release</id>
            <url>https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/</url>
        </repository>
    </repositories>

    <dependencies>

        ...

        <dependency>
            <groupId>com.ultimatesoftware</groupId>
            <artifactId>surge-engine-ks-command-javadsl_2.12</artifactId>
            <version>0.1.0</version>
        </dependency>
    </dependencies>
</project>
```

## How does it work

### Components

High level overview:

![Kafka Streams Command Components](docs/images/Surge%20Command%20Components.png)

In depth overview:
![Kafka Streams Command In Depth](docs/images/CQRS_ES%20on%20Kafka%20Streams%20with%20Kafka%20Event%20Store.png)

Each service node is responsible for a set of partitions in Kafka.  Each aggregate will
only send state to a particular partition in Kafka, and therefore belongs to a particular
"partition region" - which we will define as a collection of aggregates and per partition
actors responsible for handling any commands for aggregates of a single partition.
State updates sent to the state topic are consumed internally by the service in order to
build up an aggregate state store as a Kafka Streams KTable.  Since we are subscribing
to Kafka to build up the state store, service nodes coordinate via a Kafka consumer group
to determine which nodes are responsible for which partitions, and therefore which partition
regions live on that node.

Each service node has a single router actor, which follows updates to the consumer group
and can forward messages for a partition to the correct partition manager - using Akka tcp
messaging to send the command remotely if necessary.  Each partition region contains a
single partition manager which keeps track of which aggregate actors are running in memory
and which are not.  The partition manager forwards messages to the correct aggregate actor,
which uses wired in business logic to compute a new state and events based on the incoming command.

New state and events are persisted to 2 different Kafka topics via a stateful producer actor.
The stateful producer actor is responsible for following which aggregates have state messages
that have been persisted to Kafka but not yet consumed and reflected in the aggregate state KTable.

For an aggregate actor that is newly created (i.e. not in memory when a command is sent to it)
it must ensure that the state of the aggregate in the aggregate KTable is up to date by first
checking with the stateful producer actor if there are any in flight messages
(produced but not consumed by the Kafka Streams persistence layer) or not.  If there are,
it buffers the command and checks again after a short delay with the producer actor several
more times before giving up.  If there are no in flight messages for the aggregate, it can
query the aggregate state KTable by aggregate ID to initialize with the most up to date state
before processing the command as normal.

### Recommended Configurations

#### Aggregate idle timeout
Under the hood, we're using a Kafka Streams KTable to index aggregate state.  The KTable performs a
flush every `KAFKA_STREAMS_COMMIT_INTERVAL_MS` milliseconds (default 3000).
This flush notifies the stateful producer actor that an aggregate state in the KTable is completely
up to date.  It is therefore recommended to keep aggregates in memory for at least this long, that
way they are not impacted by the KTable flush delay.  The aggregate timeout can be configured with
`AGGREGATE_ACTOR_IDLE_TIMEOUT` and defaults to `30 seconds`.


## Running

To test:
```
sbt kafka_streams_cmd/test
```
