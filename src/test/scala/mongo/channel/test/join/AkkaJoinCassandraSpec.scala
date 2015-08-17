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

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import akka.testkit.TestKit
import join.Join
import join.cassandra.CassandraAkkaStream
import mongo.channel.test.cassandra.TemperatureEnviroment
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import com.datastax.driver.core.{ Cluster, ConsistencyLevel, Row ⇒ CRow }
import scala.concurrent.Await
import scala.concurrent.duration._

class AkkaJoinCassandraSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with TemperatureEnviroment {

  override def afterAll() = TestKit.shutdownActorSystem(system)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system)
    .withDispatcher("akka.join-dispatcher"))

  import dsl.cassandra._

  val selectSensor = "SELECT sensor FROM {0}"
  val qSensors = for { q ← select(selectSensor) } yield q

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    _ ← fk[java.lang.Long]("sensor", r.getLong("sensor"))
    q ← readConsistency(ConsistencyLevel.ONE)
  } yield q

  "CassandraJoin with Akka Streams" should {
    "perform join" in {

      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val joinSource = Join[CassandraAkkaStream].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE) { (outer, inner) ⇒
        s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"
      }

      val folder = { (acc: List[String], cur: String) ⇒ logger.info(s"consume $cur"); acc :+ cur }

      val future = joinSource.runWith(akka.stream.scaladsl.Sink.fold(List.empty[String])(folder))

      val results = Await.result(future, 5 seconds)

      if (results.size != measureSize * sensors.size)
        fail("CassandraJoin with Akka Streams")
    }
  }
}