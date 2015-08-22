/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongo.channel.test.join

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import join.Join
import akka.testkit.TestKit
import join.Joiner.AkkaConcurrentAttributes
import akka.actor.ActorSystem
import join.cassandra.CassandraAkkaStream
import mongo.channel.test.cassandra.TemperatureEnviroment
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import com.datastax.driver.core.{ Cluster, ConsistencyLevel, Row ⇒ CRow }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.util.{Failure, Success}
import scalaz.{ -\/, \/- }

class AkkaJoinCassandraSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with TemperatureEnviroment {
  implicit val dispatcher = system.dispatchers.lookup("akka.join-dispatcher")
  override def afterAll() = TestKit.shutdownActorSystem(system)

  import dsl.cassandra._

  val selectSensor = "SELECT sensor FROM {0}"
  val qSensors = for { q ← select(selectSensor) } yield q

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    _ ← fk[java.lang.Long]("sensor", r.getLong("sensor"))
    q ← readConsistency(ConsistencyLevel.ONE)
  } yield q

  def fold = { (acc: List[String], cur: String) ⇒ acc :+ cur }

  def cmb: (CassandraAkkaStream#Record, CassandraAkkaStream#Record) ⇒ String =
    (outer, inner) ⇒
      s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")} "

  "CassandraJoin with Akka Streams" should {
    "perform join" in {
      val latch = new CountDownLatch(1)
      val resRef = new AtomicReference(List[String]())
      val Mat = ActorMaterializer(ActorMaterializerSettings(system)
        .withDispatcher("akka.join-dispatcher"))

      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build
      implicit val Attributes = -\/(system)
      val joinSource =
        Join[CassandraAkkaStream].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)(cmb)

      val future = joinSource
        .runWith(Sink.fold(List.empty[String])(fold))(Mat)

      future.onComplete {
        case Success(r) ⇒
          resRef.set(r)
          latch.countDown()
        case Failure(ex) ⇒
          fail("★ ★ ★ CassandraAkkaStream sequentual has been competed with error:" + ex.getMessage)
          latch.countDown()
      }

      latch.await()
      resRef.get().size mustBe measureSize * sensors.size
    }
  }

  "CassandraJoinPar with Akka Streams" should {
    "perform parallel join" in {
      import scalaz.std.AllInstances._
      val latch = new CountDownLatch(1)
      val resRef = new AtomicReference(List[String]())

      val settings = ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher")
      implicit val Attributes = \/-(AkkaConcurrentAttributes(settings, system, 4, scalaz.Monoid[String]))

      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val parSource =
        Join[CassandraAkkaStream].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)(cmb)

      val future = parSource
        .runWith(Sink.fold(List.empty[String])(fold))(ActorMaterializer(settings)(system))

      future.onComplete {
        case Success(r) ⇒
          resRef.set(r)
          latch.countDown()
        case Failure(ex) ⇒
          fail("★ ★ ★ CassandraAkkaStream par has been competed with error::" + ex.getMessage)
          latch.countDown()
      }
      latch.await
      resRef.get().size mustBe sensors.size
    }
  }
}