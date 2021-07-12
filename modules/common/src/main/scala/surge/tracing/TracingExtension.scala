// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.{ ActorSystem, ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import akka.event.Logging
import io.opentracing.Tracer
import io.opentracing.noop.NoopTracerFactory

import scala.util.{ Failure, Success }

class TracingExtensionImpl(system: ExtendedActorSystem) extends Extension {

  private val logging = Logging(system, logSource = classOf[TracingExtensionImpl])
  private val tracingConfig = system.settings.config.getConfig("tracing")
  private val tracerFqcn = tracingConfig.getString("fqcn")

  val tracer: Tracer = system.dynamicAccess.createInstanceFor[Tracer](tracerFqcn, Nil) match {
    case Failure(ex) =>
      logging.error(ex, s"Failed to initialize tracer $tracerFqcn, falling back to no-op implementation")
      NoopTracerFactory.create()
    case Success(result: Tracer) => result
  }

}

object TracingExtension extends ExtensionId[TracingExtensionImpl] with ExtensionIdProvider {

  override def lookup: TracingExtension.type = TracingExtension
  override def createExtension(system: ExtendedActorSystem): TracingExtensionImpl = new TracingExtensionImpl(system)

  override def get(system: ActorSystem): TracingExtensionImpl = super.get(system)
  override def get(system: ClassicActorSystemProvider): TracingExtensionImpl = super.get(system)

}
