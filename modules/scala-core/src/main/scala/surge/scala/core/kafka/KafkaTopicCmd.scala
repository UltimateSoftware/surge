// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.kafka

import java.util.Properties

import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig, NewTopic }
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.JavaConverters._

trait KafkaTopicCmd {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def topics: Seq[KafkaTopic]

  private def topicObjects(defaultNumPartitions: Int, replicationFactor: Short): Seq[NewTopic] = topics.map { topic ⇒
    val numberPartitions = topic.numberPartitionsOverride.getOrElse(defaultNumPartitions)
    val topicWithNumPartitions = topic.copy(numberPartitionsOverride = Some(numberPartitions))
    TopicBuilder.kafkaTopic(topicWithNumPartitions).replicas(replicationFactor).build()
  }

  def createTopics(bootstrapServer: String): Unit = {
    val topicsToCreate = topics.map(TopicBuilder.kafkaTopic(_).build())
    createTopics(bootstrapServer, topicsToCreate)
  }

  def createTopics(bootstrapServer: String, partitions: Int, replicationFactor: Short): Unit = {
    val topicsToCreate = topicObjects(partitions, replicationFactor)
    createTopics(bootstrapServer, topicsToCreate)
  }

  private def createAdminClient(bootstrapServers: String): AdminClient = {
    val p = new Properties()
    p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    AdminClient.create(p)
  }

  private def existingTopics(client: AdminClient): scala.collection.mutable.Set[String] = {
    client.listTopics().names().get().asScala
  }

  def createTopic(bootstrapServers: String, topic: NewTopic): Unit = {
    createTopics(bootstrapServers, Seq(topic))
  }

  def createTopics(bootstrapServers: String, topics: Seq[NewTopic]): Unit = {
    val adminClient = createAdminClient(bootstrapServers)
    val existing = existingTopics(adminClient)
    val topicsToCreate = topics.filterNot(top ⇒ existing.contains(top.name()))

    adminClient.createTopics(topicsToCreate.asJavaCollection).all().get()
    topicsToCreate.foreach { topic ⇒
      log.info("Successfully created topic {}", topic.name())
    }
  }

  def deleteTopics(bootstrapServers: String, topicNames: Seq[String]): Unit = {
    val adminClient = createAdminClient(bootstrapServers)

    val existing = existingTopics(adminClient)
    val topicsToDelete = topicNames.filter(top ⇒ existing.contains(top))

    adminClient.deleteTopics(topicsToDelete.asJavaCollection).all().get()
    topicsToDelete.foreach { topic ⇒
      log.info("Successfully deleted topic {}", topic)
    }
  }
}
