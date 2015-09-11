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
import java.util.concurrent.{Executors, CountDownLatch}
import java.util.concurrent.atomic.AtomicReference

import _root_.join.Join
import _root_.join.Joiner.AkkaConcurrentAttributes
import _root_.join.cassandra.CassandraAkkaStream
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.datastax.driver.core.{Cluster, ConsistencyLevel, Row ⇒ CRow}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.util.{Failure, Success}
import scalaz.\/-

class SportCenterSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val dispatcher = system.dispatchers.lookup("akka.join-dispatcher")

  def fold = { (acc: List[String], cur: String) ⇒ acc :+ cur }

  "CassandraJoinPar with sportCenter db" should {
    "perform parallel join" in {
      import dsl.cassandra._
      import scalaz.std.AllInstances._
      import scala.collection.JavaConverters._

      val latch = new CountDownLatch(1)
      val resRef = new AtomicReference(List[String]())

      val settings = ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher")
      implicit val Attributes = \/-(AkkaConcurrentAttributes(settings, system, 4, scalaz.Semigroup[String]))

      implicit val client = Cluster.builder()
        .addContactPointsWithPorts(List(new InetSocketAddress("192.168.0.171", 9042)).asJava).build

      val qNames = for {q ← select("SELECT processor_id FROM {0}")} yield {
        q
      }

      def qDomain(r: CRow) = for {
        _ ← select("select processor_id, sequence_nr from {0} where processor_id = ? and sequence_nr > 100 and partition_nr in (0,1)")
        _ ← fk[java.lang.String]("processor_id", r.getString("processor_id"))
        q ← readConsistency(ConsistencyLevel.QUORUM)
      } yield {
          q
        }

      def cmb0: (CassandraAkkaStream#Record, CassandraAkkaStream#Record) ⇒ String =
        (outer, inner) ⇒
          s"Actor ${outer.getString("processor_id")} - ${inner.getLong("sequence_nr")} "

      val parSource = Join[CassandraAkkaStream].join(qNames, "names", qDomain, "sport_center_journal", "sport_center")(cmb0)

      val future = parSource
        .runWith(Sink.fold(List.empty[String])(fold))(ActorMaterializer(settings)(system))

      future.onComplete {
        case Success(r) ⇒
          resRef.set(r)
          latch.countDown()
        case Failure(ex) ⇒
          fail("★ ★ ★ CassandraAkkaStream par has been competed with error:" + ex.getMessage)
          latch.countDown()
      }
      latch.await
      client.close()

      println(resRef.get())
      resRef.get().size === 28
    }
  }
}