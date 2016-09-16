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

package mongo.channel.test.eventlog

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import join.cassandra.{ CassandraObservable, CassandraProcess }
import mongo.channel.test.cassandra.DomainEnviroment
import org.scalatest.{ MustMatchers, WordSpecLike }
import rx.lang.scala.Subscriber

import scala.collection.mutable
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.io
import scalaz.stream.sink.lift

class PartitionedLogCassandraSpec extends WordSpecLike with MustMatchers with DomainEnviroment {
  val pageSize = 16

  def subscriber(count: AtomicLong, latch: CountDownLatch,
                 session: CassandraObservable#Session, client: CassandraObservable#Client) = new Subscriber[CassandraObservable#Record] {

    override def onStart() = request(pageSize)

    override def onNext(row: CassandraObservable#Record) = {
      logger.info(s"${row.getString(0)}:${row.getLong(1)} ${row.getLong(2)} ★ ★ ★")
      if (count.incrementAndGet() % pageSize == 0) {
        logger.info(s"★ ★ ★ page: {} ★ ★ ★", count.get())
        request(pageSize)
      }
    }

    override def onError(ex: Throwable) = {
      println(s"★ ★ ★ CassandraObservableLog has been completed with error: ${ex.getMessage}")
      session.close()
      client.close()
      latch.countDown()
      org.scalatest.Assertions.fail(ex)
    }

    override def onCompleted() = {
      session.close()
      client.close()
      latch.countDown()
    }
  }

  "PartitionedCassandraLog" must {

    case class DbRecord(persistence_id: String, partition_nr: Long, sequence_nr: Long)

    "read log with CassandraProcess" in {

      type T = CassandraProcess
      val clusterBuilder = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)

      val client = clusterBuilder.build
      implicit val session = (client connect "journal")

      val buffer = mutable.Buffer.empty[T#Record]
      val BufferSink = io.fillBuffer(buffer)

      val Logger = lift[Task, T#Record] { row ⇒
        Task.delay {
          logger.info(s"${row.getString(0)}:${row.getLong(2)}")
        }
      }

      val Logger2 = lift[Task, Option[DbRecord]] { in ⇒
        Task.delay {
          logger.info(s"$in")
        }
      }
      val buffer2 = mutable.Buffer.empty[Option[DbRecord]]
      val BufferSink2 = io.fillBuffer(buffer2)

      //sequence_nr, partition_nr, body
      val log = (eventlog.Log[T] from (queryByKey, actors(0), 0, maxPartitionSize)).as[DbRecord]
      //.column[Long]("body")

      (for {
        row ← (eval(Task.now(session)) through log.source)
        _ ← (row observe Logger2 to BufferSink2)
      } yield ())
        .onFailure { ex ⇒
          eval_(Task.delay {
            logger.debug(s"CassandraProcessLog has been completed with error: ${ex.getMessage}")
            org.scalatest.Assertions.fail(ex)
          })
        }.onComplete {
          eval_(Task.delay {
            session.close()
            client.close()
          })
        }.runLog.run

      buffer2.length mustEqual domainSize
    }

    "read 2 logs with different lenght through zip with CassandraProcess" in {
      import scalaz.stream.Process._
      type T = CassandraProcess
      val clusterBuilder = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)

      val client = clusterBuilder.build
      implicit val session = (client connect "journal")
      val count = new AtomicLong(0l)

      def Logger2(c: AtomicLong) = lift[Task, (T#Record, T#Record)] { row ⇒
        Task.delay {
          c.getAndIncrement()
          logger.info(s"${row._1.getString(0)}:${row._1.getLong(2)} - ${row._2.getString(0)}:${row._2.getLong(2)}")
        }
      }

      val logA = (eventlog.Log[T] from (queryByKey, actors(0), 3, maxPartitionSize))
      val logB = (eventlog.Log[T] from (queryByKey, actors(1), 15, maxPartitionSize))

      (eval(Task.now(session)) through (logA zip logB).source)
        .flatMap { p ⇒
          (p to Logger2(count))
        }.onFailure { ex ⇒
          logger.debug(s"CassandraProcessLog2 has been completed with error: ${ex.getMessage}")
          org.scalatest.Assertions.fail(ex)
        }
        .onComplete {
          eval_(Task.delay {
            session.close()
            client.close()
          })
        }.runLog.run

      count.get() mustEqual domainSize - 15
    }

    "read log with CassandraObservable" in {
      val offset = 5
      val latch = new CountDownLatch(1)
      val count = new AtomicLong(0)
      val client = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)
        .build
      implicit val session = (client connect "journal")

      (eventlog.Log[CassandraObservable] from (queryByKey, actors.head, offset, maxPartitionSize))
        .observeOn(RxExecutor)
        .subscribe(subscriber(count, latch, session, client))

      latch.await(30, TimeUnit.SECONDS)

      session.close()
      client.close()
      count.get() mustEqual domainSize - offset
    }
  }
}
