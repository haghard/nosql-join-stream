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

package mongo.channel.test

import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import _root_.join.cassandra.CassandraSource
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import com.datastax.driver.core.{ Row ⇒ CRow, QueryOptions, Cluster, ConsistencyLevel }
import domain.formats.DomainEventFormats.ResultAddedFormat
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import scala.util.{ Failure, Success }

class SportCenterSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll {
  import dsl.cassandra._
  import scala.collection.JavaConverters._

  val offset = 8
  val metaInfo = 87
  val dName = "akka.join-dispatcher"

  def fold = { (acc: List[String], cur: String) ⇒ acc :+ cur }

  val idField = "persistence_id"
  val cassandraHost0 = "192.168.0.134"
  val cassandraHost1 = "192.168.0.11"
  val cassandraPort = 9042
  val settings = ActorMaterializerSettings(system).withInputBuffer(1, 1).withDispatcher(dName)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)

  def deserialize(row: CassandraSource#Record): ResultAddedFormat =
    try {
      val bts = row.getBytes("message").array()
      ResultAddedFormat parseFrom bts.slice(offset, bts.length)
    } catch {
      case e: Exception ⇒ ResultAddedFormat.getDefaultInstance
    }

  "CassandraJoinPar with sportCenter db" should {
    "perform parallel join" in {
      import feed._
      val latch = new CountDownLatch(1)
      val client = Cluster.builder()
        .addContactPointsWithPorts(List(new InetSocketAddress(cassandraHost0, cassandraPort), new InetSocketAddress(cassandraHost1, cassandraPort)).asJava)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .build

      val queryByKey = """
        |SELECT * FROM sport_center_journal WHERE
        |        persistence_id = ? AND
        |        partition_nr = ? AND
        |        sequence_nr >= ?
      """.stripMargin

      implicit val session = client.connect("sport_center")

      val cleFeed = Feed[CassandraSource].from(queryByKey, "cle", 50)
      val okcFeed = Feed[CassandraSource].from(queryByKey, "okc", 50)

      val future = (okcFeed.source ++ cleFeed.source)
        .runForeach { row ⇒
          val format = deserialize(row)
          println(s"★ ★ ★ ${format.getResult.getHomeTeam} - ${format.getResult.getAwayTeam} : ${row.getLong("sequence_nr")}")
        }

      future.onComplete {
        case Success(r) ⇒
          latch.countDown()
        case Failure(ex) ⇒
          fail("CassandraAkkaStream par has been competed with error:" + ex.getMessage)
          latch.countDown()
      }
      latch.await
      client.close()
      1 === 1
    }
  }
}