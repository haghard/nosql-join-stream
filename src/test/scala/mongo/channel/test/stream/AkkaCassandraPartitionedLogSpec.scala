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

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import join.cassandra.CassandraSource
import log.PartitionedLog
import mongo.channel.test.cassandra.DomainEnviroment
import org.scalatest.{ MustMatchers, WordSpecLike }

class AkkaCassandraPartitionedLogSpec extends TestKit(ActorSystem("akka-log-stream"))
    with WordSpecLike with MustMatchers with DomainEnviroment {

  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(1, 1)
    .withDispatcher("akka.join-dispatcher")
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup("akka.join-dispatcher")

  "FeedCassandra" must {
    "run with CassandraSource" in {
      val latch = new CountDownLatch(1)
      val count = new AtomicLong(0)

      val clusterBuilder = com.datastax.driver.core.Cluster.builder
        .addContactPointsWithPorts(cassandraHost)
        .withQueryOptions(queryOps)

      val client = clusterBuilder.build
      implicit val session = clusterBuilder.build.connect("journal")

      (PartitionedLog[CassandraSource] from (queryByKey, actors.head, 5, maxPartitionSize))
        .source
        .runForeach { row ⇒
          count.incrementAndGet()
          println(s"${row.getString(0)} : ${row.getLong(1)} ${row.getLong(2)}")
        }.onComplete(_ ⇒ latch.countDown())

      latch.await()
      session.close()
      client.close()
      count.get() mustEqual domainSize - 5
    }
  }
}
