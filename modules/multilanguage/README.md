#Surge Metrics Integration
-------------------------------
If you have access to the micro meter binder (internal UKG project)  then you can use the below example reference code to bind your

metrics to the InfluxDB else you need to provide your own Micrometer binder implementation 

so that you can bind your metrics to InfluxDB.

Follow the below steps for integrating metrics to InfluxDB :-

1 - Provide the InfluxDB config 

2 - Create InfluxMeterRegistry

3 - Bind your meter registry to your Micrometer implementation

Below is an example for reference : - 
```scala
    val config: InfluxConfig = new InfluxConfig() {
    override def org = "Org"
    override def bucket = "Bucket"
    override def token = "Token"
    // FIXME: This should be securely bound rather than hard-coded, of course.
    override def get(k: String): String = null // accept the rest of the defaults
    }

```
     
```scala
    val meterRegistry: MeterRegistry = new InfluxMeterRegistry(config, Clock.SYSTEM)
    SurgeMetricsMeterBinder.forGlobalRegistry.bindTo(meterRegistry)
    val metric:Metrics = Metrics.globalMetricRegistry
    val timer:Timer = metric.timer(MetricInfo("test","testing timer"))
    val counter1: Counter = metric.counter(MetricInfo(name = "some-custom-counter", description = "Just an example counter"))
    counter1.increment() // Increment the counter
```
   
