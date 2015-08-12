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
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.datastax.driver.core.BatchStatement
import mongo.NamedThreadFactory
import org.scalatest.{ Suite, BeforeAndAfterAll }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

trait CassandraEnviromentLifecycle extends BeforeAndAfterAll { this: Suite ⇒
  import scala.concurrent.duration._
  import scala.collection.JavaConverters._
  import org.cassandraunit.utils.EmbeddedCassandraServerHelper
  import java.lang.{ Long ⇒ JLong }

  val logger = org.apache.log4j.Logger.getLogger("Cassandra-Enviroment")
  implicit val executor = Executors.newFixedThreadPool(3, new NamedThreadFactory("cassandra-test-worker"))

  val cassandraHost = List(new InetSocketAddress("127.0.0.1", 9142)).asJava

  val LANGS = "langs"
  val PROGRAMMERS = "programmers"
  val KEYSPACE = "world"

  @tailrec private def retry[T](n: Int)(f: ⇒ T): T =
    Try(f) match {
      case Success(x) ⇒ x
      case _ if n > 1 ⇒ retry(n - 1)(f)
      case Failure(e) ⇒ throw e
    }

  val createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS world WITH replication = {
        'class' : 'SimpleStrategy',
        'replication_factor' : '3'
      }
    """
  val createTableLangs = """CREATE TABLE IF NOT EXISTS world.langs (id bigint, name text, PRIMARY KEY (id))"""

  val createTableProgrammers = """CREATE TABLE IF NOT EXISTS world.programmers (id bigint, lang bigint, name text, PRIMARY KEY (id, lang)) """

  val writeLangs = "INSERT INTO world.langs (id, name) VALUES (?, ?)"
  val writeProgrammers = "INSERT INTO world.programmers (id, name, lang) VALUES (?, ?, ?)"

  override protected def beforeAll(): Unit = {
    val f = new File(getClass.getClassLoader.getResource("cassandra_network_strategy.yaml").getPath)
    CassandraServerHelper.startEmbeddedCassandra(f, "./cas-tmp", 10.seconds.toMillis)

    val clusterBuilder = com.datastax.driver.core.Cluster.builder
      .addContactPointsWithPorts(cassandraHost)
    val cluster = clusterBuilder.build
    val session = cluster.connect()

    retry(3) { session.execute(createKeyspace) }
    session.execute(createTableLangs)
    session.execute(createTableProgrammers)

    val langs = List(session.prepare(writeLangs).bind(1l: JLong, "C++"), session.prepare(writeLangs).bind(2l: JLong, "Java")).asJava

    val programmers = List(session.prepare(writeProgrammers).bind(1l: JLong, "Jack", 1l: JLong),
      session.prepare(writeProgrammers).bind(2l: JLong, "John", 2l: JLong),
      session.prepare(writeProgrammers).bind(3l: JLong, "Jerry", 1l: JLong)).asJava

    session.execute(new BatchStatement().addAll(langs))
    session.execute(new BatchStatement().addAll(programmers))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    super.afterAll()
  }
}
