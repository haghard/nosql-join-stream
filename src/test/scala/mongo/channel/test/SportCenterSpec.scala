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

/*

package mongo.channel.test

import java.net.InetSocketAddress
import java.util.concurrent.{ Executors, CountDownLatch }
import java.util.concurrent.atomic.AtomicInteger
import _root_.join.cassandra.{ CassandraObservable, CassandraSource }
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ QueryOptions, Cluster, ConsistencyLevel }
//import domain.formats.DomainEventFormats.ResultAddedFormat
import mongo.NamedThreadFactory
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import rx.lang.scala.Subscriber
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class SportCenterSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll {

  import scala.collection.JavaConverters._

  val offset = 8
  val metaInfo = 87
  val dName = "akka.join-dispatcher"

  def fold = { (acc: List[String], cur: String) ⇒ acc :+ cur }

  val idField = "persistence_id"
  val cassandraHost0 = "192.168.0.134"
  val cassandraHost1 = "192.168.0.82"
  val cassandraPort = 9042
  val hosts = List(new InetSocketAddress(cassandraHost0, cassandraPort), new InetSocketAddress(cassandraHost1, cassandraPort)).asJava
  val settings = ActorMaterializerSettings(system).withInputBuffer(1, 1).withDispatcher(dName)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)

  implicit val executor = Executors.newFixedThreadPool(2, new NamedThreadFactory("sport-center-worker"))

  val queryByKey = """
     |SELECT * FROM sport_center_journal WHERE
     |        persistence_id = ? AND
     |        partition_nr = ? AND
     |        sequence_nr >= ?
   """.stripMargin

  def deserialize(row: CassandraSource#Record): domain.formats.DomainEventFormats.ResultAddedFormat =
    try {
      val bts = Bytes.getArray(row.getBytes("message"))
      domain.formats.DomainEventFormats.ResultAddedFormat parseFrom bts.slice(offset, bts.length)
    } catch {
      case e: Exception ⇒
        println(e.getMessage)
        domain.formats.DomainEventFormats.ResultAddedFormat.getDefaultInstance
    }

  "CassandraStream with sportCenter" should {
    "streaming with akka" in {
      val latch = new CountDownLatch(1)
      val client = Cluster.builder()
        .addContactPointsWithPorts(List(new InetSocketAddress(cassandraHost0, cassandraPort), new InetSocketAddress(cassandraHost1, cassandraPort)).asJava)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .build

      implicit val session = (client connect "sport_center")

      val cleFeed = eventlog.Log[CassandraSource] from (queryByKey, "cle", 0)
      val okcFeed = eventlog.Log[CassandraSource] from (queryByKey, "okc", 0)
      val future = (okcFeed.source ++ cleFeed.source)
        .runForeach { row ⇒
          val format = deserialize(row)
          println(s"★ ★ ★ ${format.getResult.getHomeTeam} - ${format.getResult.getAwayTeam} : ${row.getLong("sequence_nr")} ★ ★ ★")
        }

      future.onComplete {
        case Success(r) ⇒
          latch.countDown()
        case Failure(ex) ⇒
          fail("CassandraAkkaStream par has been competed with error:" + ex.getMessage)
          latch.
            countDown()
      }

      latch.await
      client.close()
      1 === 1
    }

    /*
    "CassandraObservable with sportCenter" in {
      val pageSize = 32
      val done = new CountDownLatch(1)
      val RxExecutor = ExecutionContextScheduler(ExecutionContext.fromExecutor(executor))

      val client = Cluster.builder
        .addContactPointsWithPorts(hosts)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .build

      implicit val session = (client connect "sport_center")

      val cleObs = eventlog.Log[CassandraObservable] from (queryByKey, "cle", 10)

      val S = new Subscriber[CassandraObservable#Record] {
        val count = new AtomicInteger(0)
        override def onStart() = request(pageSize)
        override def onNext(row: CassandraObservable#Record) = {
          println(s"${row.getString(0)} : ${row.getLong(1)} ${row.getLong(2)}")
          //println(deserialize(row))
          if (count.getAndIncrement() % pageSize == 0) {
            println(s"★ ★ ★ Fetched page:[$pageSize] ★ ★ ★ ")
            request(pageSize)
          }
        }
        override def onError(e: Throwable) = {
          println(s"★ ★ ★ CassandraObservableStream has been completed with error: ${e.getMessage}")
          client.close()
          done.countDown()
        }
        override def onCompleted() = {
          println("★ ★ ★ CassandraObservableStream has been completed")
          client.close()
          done.countDown()
        }
      }

      cleObs
        .observeOn(RxExecutor)
        .subscribe(S)
      done.await()
      1 === 1
    }*/
  }
}
*/
