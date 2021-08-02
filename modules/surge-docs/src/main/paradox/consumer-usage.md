# Surge Consumer Usage 

## Module Info

@@dependency[sbt,Maven,Gradle] {
symbol="SurgeVersion"
value="$project.version$"
group="com.ukg"
artifact="surge-engine-command-scaladsl_$scala.binary.version$"
version="SurgeVersion"
}

## Example

:    @@snip [Service.scala](/modules/surge-docs/src/test/scala/docs/consumer/ConsumerSpec.scala) { #consumer }
