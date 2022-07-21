// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

import javax.management.openmbean.CompositeData

trait SurgeHealthActorMBean {
  def stop(componentName: String): Unit
  def start(componentName: String): Unit
  def restart(componentName: String): Unit

  def registeredComponentNames(): java.util.List[String]
  def countRegisteredComponents(): Int
  def exists(componentName: String): Boolean
  def getHealthRegistry: CompositeData
}
