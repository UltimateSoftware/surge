// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.internal.health.{ HealthCheck, HealthCheckStatus, SurgeHealthCheck }

import scala.concurrent.ExecutionContext.Implicits.global

class SurgeHealthCheckSpec extends AnyWordSpec with Matchers with PrivateMethodTester {

  val surgeHealthCheck = new SurgeHealthCheck("surge")

  /**
   * Builds the following tree node11
   * \|____ node21
   * \|____ node31
   * \|____ node32
   * \|____ node22
   * \|____ node33
   */
  private def buildTree(statusMap: Map[String, String] = Map()): HealthCheck = {
    val node31 = HealthCheck("3.1", "3.1", statusMap.getOrElse("3.1", HealthCheckStatus.UP))
    val node32 = HealthCheck("3.2", "3.2", statusMap.getOrElse("3.2", HealthCheckStatus.UP))
    val node33 = HealthCheck("3.3", "3.3", statusMap.getOrElse("3.3", HealthCheckStatus.UP))
    // node31 and node32 are childs of node 21
    val node21 = HealthCheck("2.1", "2.1", statusMap.getOrElse("2.1", HealthCheckStatus.UP), components = Some(Seq(node31, node32)))
    // node33 is child of node 22
    val node22 = HealthCheck("2.2", "2.2", statusMap.getOrElse("2.2", HealthCheckStatus.UP), components = Some(Seq(node33)))
    // node11 is root, node21 and node22 are childs of node11
    HealthCheck("1.1", "1.1", statusMap.getOrElse("1.1", HealthCheckStatus.UP), components = Some(Seq(node21, node22)))
  }

  private def findNode(node: HealthCheck, name: String): Option[HealthCheck] = {
    if (node.name == name) {
      Some(node)
    } else {
      node.components.flatMap(_.flatMap(n => findNode(n, name)).find(_.name.equals(name)))
    }
  }

  "SurgeHealthCheck" should {
    "Calculate correctly isHealthy parameter and status if a leaf is DOWN" in {
      val root = buildTree(Map("3.1" -> HealthCheckStatus.DOWN))
      val healthyTree = PrivateMethod[HealthCheck](Symbol("healthyTree"))
      val healthyNode = surgeHealthCheck.invokePrivate(healthyTree(root))

      assert(healthyNode.isHealthy.contains(false))
      healthyNode.status shouldEqual HealthCheckStatus.DOWN

      val modifiedNode21 = findNode(healthyNode, "2.1")
      assert(modifiedNode21.exists(_.isHealthy.contains(false)))
      modifiedNode21.get.status shouldEqual HealthCheckStatus.DOWN

      val modifiedNode22 = findNode(healthyNode, "2.2")
      assert(modifiedNode22.exists(_.isHealthy.contains(true)))
      modifiedNode22.get.status shouldEqual HealthCheckStatus.UP

      val modifiedNode31 = findNode(healthyNode, "3.1")
      assert(modifiedNode31.exists(_.isHealthy.contains(false)))
      modifiedNode31.get.status shouldEqual HealthCheckStatus.DOWN

      val modifiedNode32 = findNode(healthyNode, "3.2")
      assert(modifiedNode32.exists(_.isHealthy.contains(true)))
      modifiedNode32.get.status shouldEqual HealthCheckStatus.UP

      val modifiedNode33 = findNode(healthyNode, "3.3")
      assert(modifiedNode33.exists(_.isHealthy.contains(true)))
      modifiedNode33.get.status shouldEqual HealthCheckStatus.UP
    }

    "Calculate correctly isHealthy parameter if a middle level node is DOWN" in {
      val root = buildTree(Map("2.2" -> HealthCheckStatus.DOWN))
      val healthyTree = PrivateMethod[HealthCheck](Symbol("healthyTree"))
      val healthyNode = surgeHealthCheck.invokePrivate(healthyTree(root))

      assert(healthyNode.isHealthy.contains(false))
      val modifiedNode21 = findNode(healthyNode, "2.1")
      assert(modifiedNode21.exists(_.isHealthy.contains(true)))
      val modifiedNode22 = findNode(healthyNode, "2.2")
      assert(modifiedNode22.exists(_.isHealthy.contains(false)))
      val modifiedNode31 = findNode(healthyNode, "3.1")
      assert(modifiedNode31.exists(_.isHealthy.contains(true)))
      val modifiedNode32 = findNode(healthyNode, "3.2")
      assert(modifiedNode32.exists(_.isHealthy.contains(true)))
      val modifiedNode33 = findNode(healthyNode, "3.3")
      assert(modifiedNode33.exists(_.isHealthy.contains(true)))
    }

    "Calculate correctly isHealthy parameter if all nodes are UP" in {
      val root = buildTree()
      val healthyTree = PrivateMethod[HealthCheck](Symbol("healthyTree"))
      val healthyNode = surgeHealthCheck.invokePrivate(healthyTree(root))

      assert(healthyNode.isHealthy.contains(true))
      val modifiedNode21 = findNode(healthyNode, "2.1")
      assert(modifiedNode21.exists(_.isHealthy.contains(true)))
      val modifiedNode22 = findNode(healthyNode, "2.2")
      assert(modifiedNode22.exists(_.isHealthy.contains(true)))
      val modifiedNode31 = findNode(healthyNode, "3.1")
      assert(modifiedNode31.exists(_.isHealthy.contains(true)))
      val modifiedNode32 = findNode(healthyNode, "3.2")
      assert(modifiedNode32.exists(_.isHealthy.contains(true)))
      val modifiedNode33 = findNode(healthyNode, "3.3")
      assert(modifiedNode33.exists(_.isHealthy.contains(true)))
    }
  }
}
