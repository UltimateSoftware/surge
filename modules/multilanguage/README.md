#Surge Metrics Integration
-------------------------------
In order to integrate the surge metrics to influxDb:

1 - Provide the influxdb config 

2 - create InfluxMeterRegistry

3 - Bind your meter registry to your metric binder implementation

below is an example for reference : - 

    val config: InfluxConfig = new InfluxConfig() {
    override def org = "ukg"
    override def bucket = "ukg-bucket"
    override def token = "Token"
    // FIXME: This should be securely bound rather than hard-coded, of course.
    override def get(k: String): String = null // accept the rest of the defaults
    }

if you have UKG account then you can use the below line of code to bind your

metrics to the influxdb else you need to provide your own implementation

of metric binder so that you can bind your metrics to influxdb 

    SurgeMetricsMeterBinder.forGlobalRegistry.bindTo(meterRegistry)
    val meterRegistry: MeterRegistry = new InfluxMeterRegistry(config, Clock.SYSTEM)
    val metric:Metrics = Metrics.globalMetricRegistry
    val timer:Timer = metric.timer(MetricInfo("test","testing timer",Map("customTag"->"Tag_value")))
    val counter1: Counter = metric.counter(MetricInfo(name = "some-custom-counter", description = "Just an example counter"))
    counter1.increment() // Increment the counter
