# Open Tracing Integration

Surge integrates with Open Tracing compatible platforms such as Jaeger, Zipkin etc. 

## Example Configuration

You can provide an OpenTracing `tracer` instance by overriding `tracer` in your Surge model. 


### Using Jaeger 

#### Module Info

@@dependency[sbt,Maven,Gradle] {
group="io.jaegertracing"
artifact="jaeger-client"
version="1.6.0"
}

Scala
:    @@snip [SurgeModel.scala](/modules/surge-docs/src/test/scala/docs/command/BankAccountSurgeModelWithTracer.scala) { #surge_model_class }

Java
:    Java examples coming soon...