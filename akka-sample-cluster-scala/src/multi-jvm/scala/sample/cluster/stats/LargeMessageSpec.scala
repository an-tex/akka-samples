package sample.cluster.stats

import akka.Done
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory
import sample.cluster.CborSerializable
import sample.cluster.stats.LargeMessageSpec.{Command, TypeKey, behavior}

import scala.concurrent.duration._

object LargeMessageSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  // note that this is not the same thing as cluster node roles
  val first = role("first")
  val second = role("second")
  val third = role("third")

  // this configuration will be used for all nodes
  // note that no fixed host names and ports are used
  commonConfig(ConfigFactory.parseString(
    """
    akka.actor.provider = cluster
    akka.cluster.roles = [compute]
    """).withFallback(ConfigFactory.load()))
}
// need one concrete test class per node
class LargeMessageSpecMultiJvmNode1 extends LargeMessageSpec
class LargeMessageSpecMultiJvmNode2 extends LargeMessageSpec
class LargeMessageSpecMultiJvmNode3 extends LargeMessageSpec

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

abstract class LargeMessageSpec extends MultiNodeSpec(LargeMessageSpecConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll
  with ImplicitSender {

  import LargeMessageSpecConfig._

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  implicit val typedSystem = system.toTyped

  "The Large Message Example" must {

    "illustrate how to startup cluster" in within(15.seconds) {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])

      val firstAddress = node(first).address
      val secondAddress = node(second).address
      val thirdAddress = node(third).address

      Cluster(system).join(firstAddress)

      receiveN(3).collect { case MemberUp(m) => m.address }.toSet should be(
        Set(firstAddress, secondAddress, thirdAddress))

      Cluster(system).unsubscribe(testActor)

      testConductor.enter("all-up")
    }

    def largeCommand(testProbe: TestProbe[Done]) = {
      // defaults are:
      // akka.remote.artery.advanced.maximum-frame-size = 256 KiB
      // akka.remote.artery.advanced.maximum-large-frame-size = 2 MiB
      val oneMegaByteString = Seq.fill(1024 * 1024)(".").mkString
      Command(oneMegaByteString, testProbe.ref)
    }

    // works here
    "support large messages using cluster singleton" in {
      val clusterSingleton = ClusterSingleton(typedSystem)

      runOn(first, second, third) {
        val testProbe = TestProbe[Done]()
        clusterSingleton.init(SingletonActor(behavior, "largeMessageSingleton")) ! largeCommand(testProbe)
        testProbe.expectMessage(Done)
      }
    }
    // but not here
    "support large messages using cluster sharding" in {
      val clusterSharding = ClusterSharding(typedSystem)

      runOn(first, second, third) {
        clusterSharding.init(Entity(TypeKey)(_ => behavior))
      }

      runOn(first, second, third) {
        val testProbe = TestProbe[Done]()
        clusterSharding.entityRefFor(TypeKey, "largeMessageShard") ! largeCommand(testProbe)
        testProbe.expectMessage(Done)
      }
    }
  }
}

object LargeMessageSpec {
  case class Command(data: String, replyTo: ActorRef[Done]) extends CborSerializable

  val TypeKey = EntityTypeKey[Command]("largeMessageTypeKey")

  val behavior = Behaviors.receiveMessage[Command] { command =>
    command.replyTo ! Done
    Behaviors.same
  }
}