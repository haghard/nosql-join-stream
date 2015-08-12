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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import org.scalatest.{ Matchers, WordSpecLike }
import join.cassandra.{ CassandraObservable, CassandraProcess }
import com.datastax.driver.core.{ Cluster, ConsistencyLevel, Row ⇒ CRow }
import mongo.channel.test.cassandra.CassandraEnviromentLifecycle
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.ExecutionContext
import scalaz.stream.{ Process, io }

class JoinCassandraSpec extends WordSpecLike with Matchers with CassandraEnviromentLifecycle {
  import dsl.cassandra._

  "Join with CassandraProcess" should {
    "have run" in {
      val P = Process
      val buffer = mutable.Buffer.empty[String]
      val Sink = io.fillBuffer(buffer)
      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val qLang = for {
        q ← select("SELECT id, name FROM {0}")
      } yield q

      def qProg(r: CRow) = for {
        _ ← select("SELECT * FROM {0} WHERE lang = ? allow filtering")
        _ ← fk[java.lang.Long]("id_lang", r.getLong("id"))
        q ← readConsistency(ConsistencyLevel.ONE)
      } yield q

      val query = Join[CassandraProcess].join(qLang, LANGS, qProg, PROGRAMMERS, "world") { (l, r) ⇒
        s"Pk: ${l.getLong("id")} lang: ${l.getString("name")} name: ${r.getString(2)}"
      }

      (for {
        row ← P.eval(Task.delay(client)) through query.out
        _ ← row to Sink
      } yield ())
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay { client.close(); logger.debug("Join has been completed") }) }
        .runLog.run

      logger.info("Result:" + buffer)

      if (buffer.size != 3) fail("Error in Join with CassandraProcess")
    }
  }

  "JoinG with CassandraObservable" should {
    "have run" in {
      val count = new CountDownLatch(1)
      val state = new AtomicReference(Vector[String]())
      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val qLang = for {
        q ← select("SELECT id, name FROM {0}")
      } yield q

      def qProg(r: CRow) = for {
        _ ← select("SELECT * FROM {0} WHERE lang = ? allow filtering")
        _ ← fk[java.lang.Long]("id_lang", r.getLong("id"))
        q ← readConsistency(ConsistencyLevel.ONE)
      } yield q

      val query = Join[CassandraObservable].join(qLang, LANGS, qProg, PROGRAMMERS, "world") { (l, r) ⇒
        s"Pk: ${l.getLong("id")} lang: ${l.getString("name")} name: ${r.getString(2)}"
      }

      val testSubs = new Subscriber[String] {
        override def onStart() = request(1)
        override def onNext(next: String) = {
          logger.info(s"receive $next")
          state.updateAndGet(new UnaryOperator[Vector[String]]() {
            override def apply(t: Vector[String]) = t :+ next
          })
          request(1)
        }
        override def onError(e: Throwable) = {
          logger.info(s"OnError: ${e.getMessage}")
          count.countDown()
        }
        override def onCompleted() = {
          logger.info("Join has been completed")
          client.close()
          count.countDown()
        }
      }

      query.observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
        .subscribe(testSubs)

      count.await()
      logger.info("Result:" + state)
      if (state.get().size != 3) fail("Error in Join with CassandraObservable")
    }
  }
}
