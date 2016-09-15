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
import scala.collection.mutable
import scalaz.concurrent.Task
import rx.lang.scala.Subscriber
import java.util.function.UnaryOperator
import java.util.concurrent.{ TimeUnit, CountDownLatch }
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }
import org.scalatest.{ Matchers, WordSpecLike }
import join.cassandra.{ CassandraObsFetchError, CassandraObservable, CassandraProcess, CassandraObsCursorError }
import com.datastax.driver.core.{ Row ⇒ CRow, ConsistencyLevel, QueryOptions, Cluster }
import mongo.channel.test.cassandra.TemperatureEnviroment
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.ExecutionContext
import scalaz.stream.{ Process, io }
import scalaz.stream.sink._

class JoinCassandraSpec extends WordSpecLike with Matchers with TemperatureEnviroment {
  import dsl.cassandra._

  val selectSensor = "SELECT sensor FROM {0}"
  val qSensors = for { q ← select(selectSensor) } yield q
  val RxExecutor = ExecutionContextScheduler(ExecutionContext.fromExecutor(executor))

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    q ← fk[java.lang.Long]("sensor", r.getLong("sensor"))
  } yield q

  def cmb: (CassandraObservable#Record, CassandraObservable#Record) ⇒ String =
    (outer, inner) ⇒
      s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"

  "Join with CassandraProcess" should {
    "have run" in {
      type C = CassandraProcess
      val P = Process
      val buffer = mutable.Buffer.empty[String]
      val BufferSink = io.fillBuffer(buffer)
      val LoggerS = lift[Task, String] { line ⇒ Task.delay(logger.info(line)) }

      implicit val client: C#Client = Cluster.builder()
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)
        .build

      val join = (Join[CassandraProcess] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE))(cmb)

      (for {
        row ← P.eval(Task.now(client connect KEYSPACE)) through join.source
        _ ← row observe LoggerS to BufferSink
      } yield ())
        .onFailure { ex ⇒
          logger.debug(s"CassandraProcess has been stopped with error: ${ex.getMessage}")
          P.halt
        }
        .onComplete {
          P.eval_(Task.delay {
            client.close()
            logger.info("CassandraProcess has been completed")
          })
        }
        .runLog.run

      if (buffer.size != measureSize * sensors.size)
        fail("Error in Join with CassandraProcess")
    }
  }

  "Join with CassandraObservable" should {
    "have run" in {
      val pageSize = 7
      val count = new AtomicLong(0)
      val done = new CountDownLatch(1)

      implicit val client = Cluster.builder()
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)
        .build

      val state = new AtomicReference(Vector.empty[String])
      val join = (Join[CassandraObservable] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE))(cmb)

      val S = new Subscriber[String] {
        override def onStart() = request(pageSize)
        override def onNext(next: String) = {
          logger.info(s"$next")
          state.updateAndGet(new UnaryOperator[Vector[String]]() {
            override def apply(t: Vector[String]) = t :+ next
          })
          if (count.getAndIncrement() % pageSize == 0) {
            logger.info(s"★ ★ ★ Fetched page:[$pageSize] ★ ★ ★ ")
            request(pageSize)
          }
        }
        override def onError(e: Throwable) = {
          logger.info(s"★ ★ ★ CassandraObservable has been completed with error: ${e.getMessage}")
          client.close()
          done.countDown()
        }
        override def onCompleted() = {
          logger.info("★ ★ ★ CassandraObservable has been completed")
          client.close()
          done.countDown()
        }
      }

      join
        .observeOn(RxExecutor)
        .subscribe(S)

      logger.info("Join with CassandraObservable: " + state.get())

      if (!done.await(30, TimeUnit.SECONDS)) {
        fail("Timeout error in Join with CassandraObservable")
      }

      val exp = measureSize * sensors.size
      if (count.get() != exp) {
        fail(s"Error in Join with CassandraObservable. actual:${count.get()} expected:${exp}")
      }
    }
  }

  "Join with CassandraObservable onError while we try to create a cursor" should {
    "have error" in {
      val latch = new CountDownLatch(1)
      implicit val client = Cluster.builder()
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)
        .build

      val join = (Join[CassandraObsCursorError] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE))(cmb)

      val S = new Subscriber[String] {
        override def onStart() = request(1)
        override def onNext(next: String) = request(1)
        override def onError(e: Throwable) = {
          logger.info(s"★ ★ ★  CassandraObsCursorError has been completed with error: ${e.getMessage}")
          client.close()
          latch.countDown()
        }

        override def onCompleted = {
          logger.info("★ ★ ★  CassandraObsCursorError has been completed")
          client.close()
          latch.countDown()
        }
      }

      join
        .observeOn(RxExecutor)
        .subscribe(S)

      if (!latch.await(30, TimeUnit.SECONDS)) {
        fail("Error in Join with CassandraObsCursorError")
      }
    }
  }

  "Join with CassandraObsFetchError onError while we trying to fetch records" should {
    "have error" in {
      val done = new CountDownLatch(1)
      implicit val client = Cluster.builder()
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)
        .build

      val join = (Join[CassandraObsFetchError] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE))(cmb)

      val S = new Subscriber[String] {
        override def onStart() = request(1)
        override def onNext(next: String) = request(1)
        override def onError(e: Throwable) = {
          logger.info(s"★ ★ ★  CassandraObsCursorError has been completed with error: ${e.getMessage}")
          client.close()
          done.countDown()
        }

        override def onCompleted() = {
          logger.info("★ ★ ★  CassandraObsCursorError has been completed")
          client.close()
          done.countDown()
        }
      }

      join
        .observeOn(RxExecutor)
        .subscribe(S)

      if (!done.await(15, TimeUnit.SECONDS)) {
        fail("Error in Join with CassandraObsCursorError")
      }
    }
  }
}