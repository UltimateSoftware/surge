// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.kafka

import java.util.Properties

import org.apache.kafka.clients.admin.AdminClientConfig
import org.slf4j.{ Logger, LoggerFactory }

trait KafkaTopicCli extends KafkaTopicCmd {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private sealed abstract class Cmd
  private case object CreateCmd extends Cmd
  private case object AlterCmd extends Cmd
  private case object DeleteCmd extends Cmd

  private case class TopicArgs(cmd: Option[Cmd] = None, bootstrapServer: Option[String] = None,
      numberPartitions: Option[Int] = None, replicationFactor: Option[Short] = None) {
    def props: Properties = {
      val p = new Properties()
      bootstrapServer.foreach(s ⇒ p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s))
      p
    }
  }

  private def createTopics(args: TopicArgs): Unit = {
    val partitions = args.numberPartitions.getOrElse(throw new RuntimeException("Missing required --partitions argument"))
    val replicationFactor = args.replicationFactor.getOrElse(throw new RuntimeException("Missing required --replication-factor argument"))
    val bootstrapServer = args.bootstrapServer.getOrElse(throw new RuntimeException("Missing required --bootstrap-server argument"))

    createTopics(bootstrapServer, partitions, replicationFactor)
  }

  private def deleteTopics(args: TopicArgs): Unit = {
    val bootstrapServer = args.bootstrapServer.getOrElse(throw new RuntimeException("Missing required --bootstrap-server argument"))

    deleteTopics(bootstrapServer, topics.map(_.name))
  }

  @scala.annotation.tailrec
  private def parseArgs(args: Seq[String], topicArgs: TopicArgs = TopicArgs(), unknownArgs: Seq[String] = Seq.empty): (TopicArgs, Array[String]) = args match {
    case "--create" +: other                          ⇒ parseArgs(other, topicArgs.copy(cmd = Some(CreateCmd)), unknownArgs)
    case "--alter" +: other                           ⇒ parseArgs(other, topicArgs.copy(cmd = Some(AlterCmd)), unknownArgs)
    case "--delete" +: other                          ⇒ parseArgs(other, topicArgs.copy(cmd = Some(DeleteCmd)), unknownArgs)
    case "--partitions" +: partitions +: other        ⇒ parseArgs(other, topicArgs.copy(numberPartitions = Some(partitions.toInt)))
    case "--replication-factor" +: factor +: other    ⇒ parseArgs(other, topicArgs.copy(replicationFactor = Some(factor.toShort)), unknownArgs)
    case "--bootstrap-server" +: kafkaBroker +: other ⇒ parseArgs(other, topicArgs.copy(bootstrapServer = Some(kafkaBroker)), unknownArgs)
    case Seq()                                        ⇒ (topicArgs, unknownArgs.toArray)
    case head +: tail                                 ⇒ parseArgs(tail, topicArgs, unknownArgs :+ head)
  }

  def main(args: Array[String]) {
    val (topicArgs, additionalArgs) = parseArgs(args.toSeq)

    if (topicArgs.cmd.isEmpty) {
      log.error("Missing a valid command")
      System.exit(1)
    }
    if (topicArgs.cmd.contains(CreateCmd)) {
      createTopics(topicArgs)
    } else if (topicArgs.cmd.contains(AlterCmd)) {
      log.warn("KafkaTopicsCmd --alter is not yet implemented")
      System.exit(1)
    } else if (topicArgs.cmd.contains(DeleteCmd)) {
      deleteTopics(topicArgs)
    }
  }
}
