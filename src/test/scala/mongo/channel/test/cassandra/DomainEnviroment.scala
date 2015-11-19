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

package mongo.channel.test.cassandra

import java.io.File
import com.datastax.driver.core.{ BatchStatement, Session }
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{ Suite, BeforeAndAfterAll }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.lang.{ Long ⇒ JLong }

trait DomainEnviroment extends BeforeAndAfterAll with CassandraEnviroment { this: Suite ⇒
  val DOMAIN = "domain"

  val maxPartitionSize = 16
  val domainSize = 60l

  def RxExecutor = ExecutionContextScheduler(ExecutionContext.fromExecutor(executor))

  /**
   *
   *      +-------+--------+-------+
   *      |0:body |1: body |2: body|
   * +----+-------+--------+-------+
   * |a:0 |xxx    |xxx     |xxx    |
   * +----+-------+--------+-------+
   *
   *     +------ +--------+--------+
   *     |0:body |1: body |2 :body |
   * +---+-------+--------+--------+
   * |a:1|xxx    |xxx     |xxx     |
   * +---+-------+--------+--------+
   *
   *     +------ +--------+--------+
   *     |0:body |1: body |2 :body |
   * +---+-------+--------+--------+
   * |b:0|xxx    |xxx     |xxx     |
   * +---+-------+--------+--------+
   *
   * Partition key - (persistence_id, partition_nr)
   * Clustering key - sequence_nr
   *
   */

  val createDomainTable = s"""
    CREATE TABLE IF NOT EXISTS journal.${DOMAIN} (
      persistence_id text,
      partition_nr bigint,
      sequence_nr bigint,
      body text,
      PRIMARY KEY ((persistence_id, partition_nr), sequence_nr)
     ) WITH CLUSTERING ORDER BY (sequence_nr ASC)
  """
  val actors = Vector("okc", "cle", "hou")
  val insert = s"INSERT INTO journal.${DOMAIN} (persistence_id, partition_nr, sequence_nr, body) VALUES (?, ?, ?, ?)"

  def queryByKey =
    s"""
       |SELECT * FROM $DOMAIN WHERE
       |        persistence_id = ? AND
       |        partition_nr = ? AND
       |        sequence_nr >= ?
   """.stripMargin

  def navigatePartition(sequenceNr: Long, maxPartitionSize: Long) = sequenceNr / maxPartitionSize

  def insertData(s: Session) = (0l until domainSize).flatMap { i ⇒
    actors.map(s.prepare(insert).bind(_, navigatePartition(i, maxPartitionSize): JLong, i: JLong, "xxx"))
  }.asJava

  override protected def beforeAll(): Unit = {
    val f = new File(getClass.getClassLoader.getResource("cassandra_network_strategy.yaml").getPath)
    CassandraServerHelper.startEmbeddedCassandra(f, "./cas-tmp", 10.seconds.toMillis)

    val clusterBuilder = com.datastax.driver.core.Cluster.builder
      .addContactPointsWithPorts(cassandraHost)
    val cluster = clusterBuilder.build
    val session = cluster.connect()

    executeWithRetry(2) {
      (session execute createKeyspace)
    }

    executeWithRetry(2) {
      (session execute createDomainTable)
      session.execute(new BatchStatement().addAll(insertData(session)))
      session.close()
      cluster.close()
    }

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    super.afterAll()
  }
}