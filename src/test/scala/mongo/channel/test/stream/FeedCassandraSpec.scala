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

package mongo.channel.test.stream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import join.cassandra.{ CassandraProcess, CassandraObservable }
import log.PartitionedLog
import mongo.channel.test.cassandra.DomainEnviroment
import org.scalatest.{ MustMatchers, WordSpecLike }
import rx.lang.scala.Subscriber

import scala.collection.mutable
import scalaz.concurrent.Task
import scalaz.stream.io
import scalaz.stream.sink._

class FeedCassandraSpec extends WordSpecLike with MustMatchers with DomainEnviroment {
  val pageSize = 16

  def subscriber(count: AtomicLong, latch: CountDownLatch) = new Subscriber[CassandraObservable#Record] {
    override def onStart() = request(pageSize)
    override def onNext(row: CassandraObservable#Record) = {
      println(s"${row.getString(0)} : ${row.getLong(1)} ${row.getLong(2)}")
      if (count.incrementAndGet() % pageSize == 0) {
        println(s"★ ★ ★ page : $pageSize ★ ★ ★")
        request(pageSize)
      }
    }

    override def onError(e: Throwable) = {
      println(s"★ ★ ★ CassandraObservableStream has been completed with error: ${e.getMessage}")
      latch.countDown()
    }

    override def onCompleted() = latch.countDown()
  }

  "PartitionedCassandraLog" must {
    /*
    "run with CassandraObservable" in {
      val latch = new CountDownLatch(1)
      val count = new AtomicLong(0)
      val clusterBuilder = com.datastax.driver.core.Cluster.builder.addContactPointsWithPorts(cassandraHost)
      val client = clusterBuilder.build
      implicit val session = clusterBuilder.build.connect("journal")

      (PartitionedLog[CassandraObservable] from (queryByKey, actors.head, 5, maxPartitionSize))
        .observeOn(RxExecutor)
        .subscribe(subscriber(count, latch))

      latch.await()
      client.close()
      count.get() mustEqual domainSize - 5
    }*/

    "run with CassandraProcess" in {
      import scalaz.stream.Process._
      type T = CassandraProcess
      val clusterBuilder = com.datastax.driver.core.Cluster.builder.addContactPointsWithPorts(cassandraHost)
      val client = clusterBuilder.build
      implicit val session = (client connect "journal")

      val buffer = mutable.Buffer.empty[T#Record]
      val BufferSink = io.fillBuffer(buffer)
      val Logger = lift[Task, T#Record] { row ⇒
        Task.delay(logger.info(s"${row.getString(0)}: ${row.getLong(1)}: ${row.getLong(2)}"))
      }

      val log = (PartitionedLog[T] from (queryByKey, actors.head, 5, maxPartitionSize))

      (for {
        row ← (eval(Task.now(session)) through log.out)
        _ ← row observe Logger to BufferSink
      } yield ())
        .onFailure { ex ⇒ logger.debug(s"CassandraProcess has been completed with error: ${ex.getMessage}"); scalaz.stream.Process.halt }
        .onComplete {
          eval_(Task.delay {
            session.close()
            client.close()
            logger.info("★ ★ ★ CassandraProcess has been completed")
          })
        }.runLog.run

      buffer.length mustEqual domainSize - 5
    }
  }
}