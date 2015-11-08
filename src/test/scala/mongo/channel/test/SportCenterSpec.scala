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
/*

import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import _root_.join.Join
import _root_.join.cassandra.CassandraSource
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ Row ⇒ CRow, QueryOptions, Cluster, ConsistencyLevel }
import domain.formats.DomainEventFormats.ResultAddedFormat
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import scala.annotation.tailrec
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
  val cassandraHost = "192.168.0.134"
  val cassandraPort = 9042
  val settings = ActorMaterializerSettings(system).withInputBuffer(1, 1).withDispatcher(dName)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)

  def cmb: (CassandraSource#Record, CassandraSource#Record) ⇒ CassandraSource#Record =
    (outer, inner) ⇒
      inner

  val qNames = for {
    q ← select(s"""SELECT processor_id FROM {0}""")
  } yield q

  //and sequence_nr > 150
  def qDomain(r: CRow) = for {
    _ ← select(s"select $idField, sequence_nr, message from {0} where $idField = ? and partition_nr in (0,1)")
    q ← fk[java.lang.String](idField, r.getString("processor_id"))
  } yield q

  def deserialize(row: CassandraSource#Record): ResultAddedFormat =
    try {
      (ResultAddedFormat parseFrom row.getBytes("message").array())
    } catch {
      case e: Exception ⇒ ResultAddedFormat.getDefaultInstance
    }

  "CassandraJoinPar with sportCenter db" should {
    "perform parallel join" in {
      val latch = new CountDownLatch(1)
      implicit val client = Cluster.builder()
        .addContactPointsWithPorts(List(new InetSocketAddress(cassandraHost, cassandraPort)).asJava)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .build

      val parSource = (Join[CassandraSource] left (qNames, "teams", qDomain, "sport_center_journal", "sport_center"))(cmb)

      val future = parSource.source
        .filter(_.getString(idField) == "cle")
        .runForeach { row ⇒
          println(s"${row.getString(idField)}:${row.getLong("sequence_nr")}")
          val format = deserialize(row)
          println(s"★ ★ ★ ${format.getResult.getHomeScore} ★ ★ ★")
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
}*/
