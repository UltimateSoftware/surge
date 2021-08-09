# Command Side Usage

Surge provides an engine that must be configured with domain logic.  It uses this domain logic
under the hood to determine what events should be emitted based on a command and how to update
the state of an aggregate based on those events.  This page describes how to configure, create,
and interact with a Surge command engine.

## Module Info

@@dependency[sbt,Maven,Gradle] {
  symbol="SurgeVersion"
  value="$project.version$"
  group="com.ukg"
  artifact="surge-engine-command-scaladsl_$scala.binary.version$"
  version="SurgeVersion"
}

## Domain Logic

### Aggregates

An aggregate type is the base type for an instance of the Surge engine.  Multiple Surge engines can be run within a service, but one instance of the engine should map to exactly one type of aggregate.
Aggregates should be simple data classes used to maintain any context/state needed to process incoming commands.

@@@ note

Please, make sure your Aggregate inherits `java.io.Serializable` to guarantee it gets serialized and deserialized correctly.

@@@

Scala
:    @@snip [Aggregate.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandModel.scala) { #aggregate_class }

Java
:    @@snip [Aggregate.java](/modules/surge-docs/src/test/java/docs/example/account/BankAccount.java) { #aggregate_class }

### Commands

Commands are data classes intended to handled by an instance of an aggregate to emit events or reject the command and return an error to the end user.
Surge strongly types a command engine on an aggregate type, command type, and event type, so any commands you intend to send to an aggregate must inherit from the same base class/interface.

Scala
:    @@snip [Command.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandModel.scala) { #command_class }

Java
:    Java examples coming soon...

### Events

Events are the result of successfully applied commands.

Scala
:    @@snip [Event.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandModel.scala) { #event_class }

Java
:    Java examples coming soon...

## Surge Configuration

The configuration of a Surge engine is done by implementing a command model interface with your particular domain logic and a Surge model interface with Kafka topic configuration.

The command model interface is a way to express the way a domain should handle commands and process events for a particular instance of an aggregate.  This is the main wiring of domain logic that Surge leverages.
It is strongly typed on aggregate Id type, aggregate, base command, and base event type.

Scala
:    @@snip [CommandModel.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandModel.scala) { #command_model_class }

Java
:    Java examples coming soon...

The command model gets wired into Surge through a Surge model, which additionally includes the topics to publish to.

Scala
:    @@snip [SurgeModel.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountSurgeModel.scala) { #surge_model_class }

Java
:    Java examples coming soon...


## Creating and interacting with the engine

Once the configuration is defined you can inject it into a Surge engine:

Scala
:    @@snip [Engine.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountEngine.scala) { #bank_account_engine_class }

Java
:    Java examples coming soon...


You can interact with the running engine via two main methods, `sendCommand` for sending commands and `getState` for fetching state:

Scala
:    @@snip [Service.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandEngineSpec.scala) { #sending_command_to_engine }

Java
:    Java examples coming soon...

The `sendCommand` method will return a `CommandSuccess` if the command was processed successfully, even if no events are emitted or a `CommandFailure` wrapping any exception thrown as part of the command processing logic if one is thrown.

Scala
:    @@snip [Service.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountCommandEngineSpec.scala) { #getting_state_from_engine }

Java
:    Java examples coming soon...

The `getState` method will return the current state of the given aggregate if it exists.
