# Surge

**WARNING** Surge is currently in a pre-release state. There are a lot of great ideas and solid components in this repository, but we are still actively working and shaping the libraries as we move towards a more stable and official 1.0 release. For more details, see what you can [currently expect for support and stability](CURRENT_SUPPORT.md).

Many stateful message driven applications today leverage Kafka as a message bus, but tend to keep state stored in a data store outside of Kafka. Keeping
this separate data store adds a bit of extra complexity, maintenance overhead, and in bad fault conditions, synchronization issues between the message bus
and state store. Surge aims to simplify this by moving the state store to Kafka - providing an engine for stateful streaming and CQRS applications to
operate purely on Kafka.

## Why Surge?

By leveraging Kafka exclusively for maintaining state, we eliminate the additional overhead of an additional data store exclusively for maintaining state.
Removing the additional data store from the equation, services can lean on async Kafka replication for Disaster Recovery - taking your RPO and RTO from the hours
a traditional backup may take to the few seconds it might take to replicate a message in real time over Kafka. Additionally, removing this additional data store
can eliminate problems that may occur when the data store itself is lost and must be restored.  In these cases, your service may have published some events
that are now not reflected in your restored data store - leading to confusion and at worst corrupt data leaving your service onto the event stream. By leveraging Surge,
you get a highly scalable, resilient, reactive mechanism for state management purely on Kafka that simply avoids all of these potential problems by ditching
traditional state storage in favor of bringing it to where the rest of your data lives anyways - Kafka.

## Reference Documentation

TODO: Help wanted to fill in this section

## Contributing

Contributions are welcome!

Check out our [contributing guidelines](CONTRIBUTING.md) for more details about the process. Feel free to additionally ask for clarifications or guidance in GitHub issues directly.
