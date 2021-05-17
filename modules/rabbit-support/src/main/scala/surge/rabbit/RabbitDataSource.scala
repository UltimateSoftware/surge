// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import java.util.Collections

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{ AmqpSource, CommittableReadResult }
import akka.stream.scaladsl.{ Flow, Keep }
import akka.util.ByteString
import org.slf4j.{ Logger, LoggerFactory }
import surge.streams.{ DataHandler, DataPipeline, DataSource, EventPlusStreamMeta }

import scala.jdk.CollectionConverters._

object RabbitDataSource {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

trait RabbitDataSource[Key, Value] extends DataSource {
  import RabbitDataSource._
  def actorSystem: ActorSystem

  def rabbitMqUri: String

  def queueName: String

  def autoDeclarePlan: Option[AutoDeclarePlan] = None

  def readResultToKey: CommittableReadResult => Key

  def readResultToValue: ByteString => Value

  def restartBackoff: Option[RestartBackoff] = None

  def bufferSize: Int

  private val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)

  private def businessFlow(sink: DataHandler[Key, Value]): Flow[CommittableReadResult, CommittableReadResult, NotUsed] = {
    val handleEventFlow = Flow[CommittableReadResult]
      .map { crr =>
        val key = readResultToKey(interceptReadResult(crr))
        val value = readResultToValue(crr.message.bytes)
        val headers = Option(crr.message.properties.getHeaders).getOrElse(Collections.emptyMap()).asScala.map(tup => tup._1 -> tup._2.toString.getBytes()).toMap
        val eventPlusMeta = EventPlusStreamMeta(key, value, streamMeta = None, headers)
        eventPlusMeta
      }
      .via(sink.dataHandler)

    Flow[CommittableReadResult].via(PassThroughFlow(handleEventFlow, Keep.right))
  }

  def to(sink: DataHandler[Key, Value]): DataPipeline = {
    val settings = NamedQueueSourceSettings(connectionProvider, queueName)

    val source = autoDeclarePlan match {
      // If plan defined, perform declarations.
      case Some(plan) =>
        AmqpSource.committableSource(settings.withDeclarations(declarations(plan)), bufferSize).via(businessFlow(sink))
      // If no plan defined, don't perform declarations.
      case None =>
        AmqpSource.committableSource(settings, bufferSize).via(businessFlow(sink))
    }

    new RabbitDataPipeline(source, actorSystem, restartBackoff)
  }

  protected[rabbit] def logDeclarations(declarations: Seq[Declaration]): Seq[Declaration] = {
    log.debug("RabbitDataSource Declarations")
    declarations.foreach(d => log.debug("declaration => ", d))
    declarations
  }

  protected[rabbit] def joinDeclarations(declarations: Declaration*): scala.collection.immutable.Seq[Declaration] = {
    logDeclarations(declarations.toSeq).toVector
  }

  protected[rabbit] def declarations(plan: AutoDeclarePlan): scala.collection.immutable.Seq[Declaration] = {
    joinDeclarations(plan.queuePlan.declaration(), plan.exchangePlan.declaration(), plan.binding.declaration())
  }

  protected[rabbit] def interceptReadResult(result: CommittableReadResult): CommittableReadResult = {
    result
  }
}
