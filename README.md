# Surge Kafka Streams
This repository houses the code for the Surge command engine,
the Surge query engine, and components shared between the two.

See the [Surge docs page](https://docs.ulti.io/systems/surge/) for more details about Surge and how to use it.

TODO Move me to the docs pages
### Recommended Configurations

#### Aggregate idle timeout
Under the hood, we're using a Kafka Streams KTable to index aggregate state.  The KTable performs a
flush every `KAFKA_STREAMS_COMMIT_INTERVAL_MS` milliseconds (default 3000).
This flush notifies the stateful producer actor that an aggregate state in the KTable is completely
up to date.  It is therefore recommended to keep aggregates in memory for at least this long, that
way they are not impacted by the KTable flush delay.  The aggregate timeout can be configured with
`AGGREGATE_ACTOR_IDLE_TIMEOUT` and defaults to `30 seconds`.

### Testing Applications

When running integration tests with Surge, you'll need to set a couple
of configuration settings to ensure that instances of the application spun
up for integration testing run in isolation.

In the `src/test/resources` directory for your project, add a file named
`application.properties`.  In that file, set the following settings:
```
# This setting configures the Kafka streams instances to use a random consumer group so that each test
# gets its own instance of persisted state stores.
kafka.streams.test-mode = true

# This setting configures the underlying Akka actors to use a random open port rather than an assigned port.
akka.remote.artery.canonical.port = 0
```

### Configuring Kafka Security

Kafka security can be configured through either overrides in an application.conf file or through environment variables.

| Config Setting | Environment Variable | Description |
| -------------- | -------------------- | ----------- |
| kafka.security.protocol | KAFKA_SECURITY_PROTOCOL | Security protocol to use, ex. SSL, SASL_SSL, or PLAINTEXT |
| kafka.ssl.truststore.location | KAFKA_SSL_TRUSTSTORE_LOCATION | The password for the trust store file |
| kafka.ssl.truststore.password | KAFKA_SSL_TRUSTSTORE_PASSWORD | The location on disk of the trust store file |
| kafka.sasl.mechanism | KAFKA_SASL_MECHANISM | SASL mechanism used for client connections, defaults to GSSAPI |
| kafka.sasl.kerberos.service.name | KAFKA_SASL_KERBEROS_SERVICE_NAME | The Kerberos principal name that Kafka runs as |
| kafka.ssl.keystore.location | KAFKA_SSL_KEYSTORE_LOCATION | The location of the key store file |
| kafka.ssl.keystore.password | KAFKA_SSL_KEYSTORE_PASSWORD | The store password for the key store file |
| kafka.ssl.key.password | KAFKA_SSL_KEY_PASSWORD | The password of the private key in the keystore file |

Note: Using SASL_SSL as the protocol you'll need to use the `kafka.sasl` settings but not the `kafka.ssl.keystore` settings.
Using SSL as the protocol you'll need the `kafka.ssl.keystore` settings but not the `kafka.sasl` settings.  Both protocols
will need configuration for the `kafka.ssl.truststore` settings.

You'll additionally need a Java Authentication and Authorization Service (JAAS) configuration, e.g.: kafka_client_jaas.conf, a Kerberos
keytab file, and a Kerberos config file.  You'll need to set a couple of JAVA_OPTS/flags when running the service to point the Java runtime to
a couple of these files:
```
-Djava.security.auth.login.config (location of the JAAS file)
-Djava.security.krb5.conf (location of the krb5.conf file)
```

## Running

To test:
```
sbt kafka_streams_cmd/test
```
