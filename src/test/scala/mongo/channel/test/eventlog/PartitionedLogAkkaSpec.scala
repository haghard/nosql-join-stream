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

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import join.cassandra.CassandraSource
import log.CassandraAsyncLog
import mongo.channel.test.cassandra.DomainEnviroment
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Span }
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.concurrent.Await
import scala.concurrent.duration._

class PartitionedLogAkkaSpec extends TestKit(ActorSystem("akka-log-stream"))
  with WordSpecLike with MustMatchers with DomainEnviroment with ScalaFutures {
  val buffer = 1 << 5
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(buffer, buffer)
    .withDispatcher("akka.join-dispatcher")
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup("akka.join-dispatcher")

  val cfg = PatienceConfig(timeout = scaled(Span(3000, Millis)), interval = scaled(Span(500, Millis)))

  "EventLogCassandra" must {

    "run CassandraSource" in {
      val offset = 5
      implicit val session = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps).build connect "journal"

      val rows = (eventlog.Log[CassandraSource] from (queryByKey, actors.head, offset, maxPartitionSize))
        .source
        .runWith(Sink.seq)
        .futureValue

      session.close
      rows.map(_.getLong("sequence_nr")) must have size domainSize - offset
    }

    "run CassandraAsyncSource" in {
      val offset = 1025
      implicit val session = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps).build connect "journal"

      val rows = CassandraAsyncLog(session, 512, queryByKey, actors.head, offset, maxPartitionSize)
        .runWith(Sink.seq)
        .futureValue

      session.close
      rows.map(_.getLong("sequence_nr")) must have size domainSize - offset
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    println("Stopped ActorSystem")
    Await.result(system.terminate(), 5.seconds)
  }
}