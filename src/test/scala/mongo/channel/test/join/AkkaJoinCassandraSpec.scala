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

import join.Join
import akka.testkit.TestKit
import join.Joiner.AkkaConcurrentAttributes
import scala.concurrent.Await
import akka.actor.ActorSystem
import scala.concurrent.duration._
import join.cassandra.CassandraAkkaStream
import mongo.channel.test.cassandra.TemperatureEnviroment
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import com.datastax.driver.core.{ Cluster, ConsistencyLevel, Row ⇒ CRow }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scalaz.{ -\/, \/- }

class AkkaJoinCassandraSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with TemperatureEnviroment {

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
  def cmb: (CRow, CRow) ⇒ String =
    (outer, inner) ⇒
      s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")} "

  "CassandraJoin with Akka Streams" should {
    "perform join" in {
      val Mat = ActorMaterializer(ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher"))
      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build
      implicit val Attributes = -\/(system)
      val joinSource =
        Join[CassandraAkkaStream].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)(cmb)

      val future = joinSource
        .runWith(Sink.fold(List.empty[String])(fold))(Mat)

      val results = Await.result(future, 10 seconds)

      if (results.size != measureSize * sensors.size)
        fail(s"CassandraJoin with Akka Streams")
    }
  }

  "CassandraJoinPar with Akka Streams" should {
    "perform parallel join" in {
      import scalaz.std.AllInstances._
      val settings = ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher")
      implicit val Attributes = \/-(AkkaConcurrentAttributes(settings, system, 4, scalaz.Monoid[String]))

      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val parSource =
        Join[CassandraAkkaStream].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)(cmb)

      val future = parSource
        .runWith(Sink.fold(List.empty[String])(fold))(ActorMaterializer(settings)(system))

      val results = Await.result(future, 10 seconds)
      if (results.size != sensors.size)
        fail("CassandraJoinPar with Akka Streams")
    }
  }
}