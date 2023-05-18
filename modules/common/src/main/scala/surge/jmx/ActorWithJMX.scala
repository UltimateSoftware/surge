// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.jmx

import akka.actor.{ Actor, ActorLogging }

import javax.management.{ InstanceAlreadyExistsException, MBeanServer, ObjectName }
import scala.util.Try

trait ActorWithJMX extends Actor with ActorLogging {
  import java.lang.management.ManagementFactory
  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  val objName = new ObjectName(
    managementName(), {
      import scala.jdk.CollectionConverters._
      new java.util.Hashtable(Map("name" -> self.path.toStringWithoutAddress, "type" -> getMXTypeName).asJava)
    })

  def managementName(): String
  def getMXTypeName: String

  private def mbs(): MBeanServer = mbeanServer

  override def preStart(): Unit = Try {
    mbs().registerMBean(this, objName)
  }.recover { case _: InstanceAlreadyExistsException =>
    log.info("JmxBean Already Registered - {}", objName)
  }

  override def postStop(): Unit = mbs().unregisterMBean(objName)

}
