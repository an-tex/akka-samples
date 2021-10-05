package sample.cluster.stats

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import sample.cluster.CborSerializable
import sample.cluster.stats.LargeMessageSpec.{LargeMessageResponse, behavior, oneMegaByteString}

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
    akka.remote.artery.large-message-destinations = [
      ## none of these match the temporary ask actor /temp/singletonProxylargeMessageSingleton-no-dc$a
      "/temp/singletonProxy*",
      "/temp/singletonProxylargeMessageResponseSingleton*",
      "/temp/singletonProxylargeMessageResponseSingleton**",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc$**",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc\\$*",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc**",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc*",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc/*",
      "/temp/singletonProxylargeMessageResponseSingleton-no-dc/**",
      ## only full wildcard
      #"/temp/*",
      ## or hardcoding like this works :(
      #"/temp/singletonProxylargeMessageResponseSingleton-no-dc$a",

    ]
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

    "support large messages in reply to temporary ask actor from cluster singleton" in {
      val clusterSingleton = ClusterSingleton(typedSystem)

      runOn(first, second, third) {
        val singleton = clusterSingleton.init(SingletonActor(behavior, "largeMessageResponseSingleton"))
        val eventualString = singleton.ask(LargeMessageResponse)(3.seconds, implicitly[Scheduler])
        eventualString.futureValue(PatienceConfiguration.Timeout(4.seconds)).length shouldBe oneMegaByteString.length
      }
    }
  }
}

object LargeMessageSpec {
  case class LargeMessageResponse(replyTo: ActorRef[String]) extends CborSerializable

  val oneMegaByteString = Seq.fill(1024 * 1024)(".").mkString

  val behavior = Behaviors.receiveMessage[LargeMessageResponse] { command =>
    command.replyTo ! oneMegaByteString
    Behaviors.same
  }
}