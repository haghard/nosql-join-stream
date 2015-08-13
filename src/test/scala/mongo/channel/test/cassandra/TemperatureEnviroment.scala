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
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.{ Failure, Success, Try }

trait TemperatureEnviroment extends BeforeAndAfterAll { this: Suite ⇒
  import scala.concurrent.duration._
  import scala.collection.JavaConverters._
  import org.cassandraunit.utils.EmbeddedCassandraServerHelper
  import java.lang.{ Long ⇒ JLong }
  import java.lang.{ Double ⇒ JDouble }

  val logger = org.apache.log4j.Logger.getLogger("Сassandra-Consumer")
  implicit val executor = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new NamedThreadFactory("cassandra-worker"))

  val cassandraHost = List(new InetSocketAddress("127.0.0.1", 9142)).asJava

  val TEMPERATURE = "temperature_by_sensor"
  val SENSORS = "sensors"
  val KEYSPACE = "journal"

  val sensors: List[JLong] = List(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l)

  val measureSize = 100

  @tailrec private def retry[T](n: Int)(f: ⇒ T): T =
    Try(f) match {
      case Success(x) ⇒ x
      case _ if n > 1 ⇒ retry(n - 1)(f)
      case Failure(e) ⇒ throw e
    }

  val createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS journal
      WITH replication = {  'class' : 'SimpleStrategy', 'replication_factor' : '3'  }
    """

  /**
   * Sharded key (day, sensor)
   * Clustered key event_time
   * Optimized for search based on (day, sensor)
   *
   */
  /*
  val createTableTemperature =
    """CREATE TABLE IF NOT EXISTS journal.temperature_by_day
      |(day text, sensor uuid, event_time timestamp, temperature double
      |PRIMARY KEY ((day, sensor), event_time))
      |WITH CLUSTERING ORDER by event_time DESC""".stripMargin
  */

  /**
   *
   * Row size = {{sensors.size}}
   * All measures by sensor are grouped in single row
   *
   *     +----+----+----+
   *     |111 |112 |113 |
   * +---+----+----+----+
   * | 1 |32.1|35.9|36.9|
   * +---+----+----+----+
   *
   *     +----+----+----+
   *     |111 |112 |113 |
   * +---+----+----+----+
   * | 2 |37.0|36.0|37.7|
   * +---+----+----+----+
   *
   * Sharded key sensor
   * Clustered key event_time
   *
   * Optimal structure for filter/search temperature by sensor
   *
   * Warning: bounded to 2 billion
   */
  val createTableTemperatureBySensor = s"""
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.${TEMPERATURE} (
        sensor bigint,
        event_time bigint,
        temperature double,
        PRIMARY KEY (sensor, event_time)
      ) WITH CLUSTERING ORDER by (event_time DESC)
  """

  val createReportedSensors = s"""
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.${SENSORS} (
        sensor bigint,
        description text,
        PRIMARY KEY (sensor))
  """

  val writeSensors = s"INSERT INTO ${KEYSPACE}.${SENSORS} (sensor, description) VALUES (?, ?)"
  val writeTemperature = s"INSERT INTO ${KEYSPACE}.${TEMPERATURE} (sensor, event_time, temperature) VALUES (?, ?, ?)"

  override protected def beforeAll(): Unit = {
    val f = new File(getClass.getClassLoader.getResource("cassandra_network_strategy.yaml").getPath)
    CassandraServerHelper.startEmbeddedCassandra(f, "./cas-tmp", 10.seconds.toMillis)

    val clusterBuilder = com.datastax.driver.core.Cluster.builder
      .addContactPointsWithPorts(cassandraHost)
    val cluster = clusterBuilder.build
    val session = cluster.connect()

    retry(3) {
      session.execute(createKeyspace)
    }
    session.execute(createReportedSensors)
    session.execute(createTableTemperatureBySensor)

    val sensorsInsert = sensors.map(id ⇒ session.prepare(writeSensors).bind(id: JLong, s"Description for sensor $id")).asJava

    def temperature(): JDouble = ThreadLocalRandom.current().nextDouble(40.9d)

    val metersData = (1 to measureSize).flatMap { i ⇒
      val ts = System.currentTimeMillis()
      sensors.map(session.prepare(writeTemperature).bind(_: JLong, ts: JLong, temperature()))
    }.asJava

    session.execute(new BatchStatement().addAll(sensorsInsert))
    session.execute(new BatchStatement().addAll(metersData))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    super.afterAll()
  }
}