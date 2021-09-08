// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

import akka.actor.ActorRef
import surge.health.jmx.Domain.HealthRegistrationDetail

import java.beans.ConstructorProperties
import java.util
import javax.management.openmbean._
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object View {

  object HealthRegistryMxView {
    def compositeType(): CompositeType = {
      new CompositeType("HealthRegistry", "Live Registry", itemNames(), itemDescriptions(), itemTypes())
    }

//    def tabularType(): TabularType = {
//      new TabularType(
//        "HealthRegistrationDetails",
//        "Existing Health Registration Details",
//        HealthRegistrationDetailMxView.compositeType(),
//        HealthRegistrationDetailMxView.itemNames())
//    }

    def arrayType(): ArrayType[CompositeData] = {
      new ArrayType[CompositeData](1, HealthRegistrationDetailMxView.compositeType())
    }

    def itemNames(): Array[String] = {
      Seq("registrations", "sender").toArray
    }

    def itemDescriptions(): Array[String] = {
      Seq("Existing Health Registrations", "Sender").toArray
    }

    def itemTypes(): Array[OpenType[_]] = {
      Array[OpenType[_]](arrayType(), SimpleType.STRING)
    }

    def scalaSeqToJavaList(data: Seq[HealthRegistrationDetailMxView]): java.util.List[HealthRegistrationDetailMxView] = {
      val list = new util.ArrayList[HealthRegistrationDetailMxView]()
      data.foreach(view => list.add(view))

      list
    }

    def apply(details: Seq[HealthRegistrationDetailMxView], sender: ActorRef): HealthRegistryMxView = {
      new HealthRegistryMxView(scalaSeqToJavaList(details), sender.path.name)
    }
  }

  class HealthRegistryMxView @ConstructorProperties(Array("registrations", "sender")) private (
      @BeanProperty val registrations: util.List[HealthRegistrationDetailMxView],
      @BeanProperty val sender: String) {
    import HealthRegistryMxView._
    import scala.jdk.CollectionConverters._

    def asCompositeData(): CompositeData = {
      new CompositeDataSupport(compositeType(), itemNames(), Array[Object](asTabularData(), sender))
    }

    private def asArrayData(): Array[CompositeData] = {
      val data: ArrayBuffer[CompositeData] = ArrayBuffer[CompositeData]()
      registrations.asScala.foreach(r => data.append(r.asCompositeData()))
      data.toArray
    }

    private def asTabularData(): TabularData = {
      val data: TabularDataSupport = new TabularDataSupport(
        new TabularType(
          "HealthRegistrationDetailMxView",
          "Health Registration Data",
          HealthRegistrationDetailMxView.compositeType(),
          HealthRegistrationDetailMxView.itemNames()))
      data.putAll(registrations.asScala.map(details => details.asCompositeData()).toArray)
      data
    }
  }

  object HealthRegistrationDetailMxView {
    def itemNames(): Array[String] = {
      Seq("componentName", "controlRefPath").toArray
    }

    def itemDescriptions(): Array[String] = {
      Seq[String]("Name of Component", "Path to Actor Control").toArray
    }

    def itemTypes(): Array[OpenType[_]] = {
      Seq[OpenType[_]](SimpleType.STRING, SimpleType.STRING).toArray
    }

    def compositeType(): CompositeType = {
      new CompositeType(
        "HealthRegistrationDetailMxView",
        "Details of a specific Health Registration",
        HealthRegistrationDetailMxView.itemNames(),
        HealthRegistrationDetailMxView.itemDescriptions(),
        HealthRegistrationDetailMxView.itemTypes())
    }

    def apply(detail: HealthRegistrationDetail): HealthRegistrationDetailMxView = {
      new HealthRegistrationDetailMxView(detail.componentName, detail.controlRefPath)
    }
  }

  class HealthRegistrationDetailMxView @ConstructorProperties(Array("componentName", "sender")) private (
      @BeanProperty val componentName: String,
      @BeanProperty val sender: String) {
    import HealthRegistrationDetailMxView._
    def asCompositeData(): CompositeData = {
      new CompositeDataSupport(compositeType(), itemNames(), Array(componentName, sender))
    }
  }
}
