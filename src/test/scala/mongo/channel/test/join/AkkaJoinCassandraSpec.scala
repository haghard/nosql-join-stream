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
import akka.actor.ActorSystem
import join.cassandra.CassandraSource
import mongo.channel.test.cassandra.TemperatureEnviroment
import akka.stream.{ Supervision, ActorMaterializerSettings, ActorMaterializer }
import com.datastax.driver.core.{ Row ⇒ CRow, Cluster }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import scala.util.{ Failure, Success }

class AkkaJoinCassandraSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
  with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with TemperatureEnviroment {

  import dsl.cassandra._

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val selectSensor = "SELECT sensor FROM {0}"
  val qSensors = for { q ← select(selectSensor) } yield q

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    q ← fk[java.lang.Long]("sensor", r.getLong("sensor"))
  } yield q

  def cmb: (CassandraSource#Record, CassandraSource#Record) ⇒ String =
    (outer, inner) ⇒
      s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")} "

  def decider(c: CassandraSource#Client): Supervision.Decider = {
    case _ ⇒
      c.close()
      Supervision.Stop
  }

  val dName = "akka.join-dispatcher"
  implicit val dispatcher = system.dispatchers.lookup(dName)

  "CassandraJoin with Akka Streams" should {
    "perform join" in {
      implicit val c = Cluster.builder()
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps).build

      val settings = ActorMaterializerSettings(system)
        .withInputBuffer(32, 64)
        .withDispatcher(dName)
        .withSupervisionStrategy(decider(c))
      implicit val Mat = ActorMaterializer(settings)

      val latch = new CountDownLatch(1)
      val resRef = new AtomicReference(List[String]())

      val join = (Join[CassandraSource] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE))(cmb)

      val future = join.source.runFold(List.empty[String]) { (acc, cur) ⇒ cur :: acc }

      future.onComplete {
        case Success(r) ⇒
          resRef.set(r)
          latch.countDown()
          c.close
        case Failure(ex) ⇒
          fail("★ ★ ★ CassandraAkkaStream join has been competed with error:" + ex.getMessage)
          latch.countDown()
      }

      latch.await()
      resRef.get().size mustBe (measureSize * sensors.size)
    }
  }
}