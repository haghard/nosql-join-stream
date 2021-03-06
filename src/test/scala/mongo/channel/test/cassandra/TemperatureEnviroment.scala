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
import com.datastax.driver.core.{ ConsistencyLevel, QueryOptions, BatchStatement }
import org.scalatest.{ Suite, BeforeAndAfterAll }

import scala.concurrent.forkjoin.ThreadLocalRandom

trait TemperatureEnviroment extends CassandraEnviroment with BeforeAndAfterAll { this: Suite ⇒
  import scala.collection.JavaConverters._
  import java.lang.{ Long ⇒ JLong }
  import java.lang.{ Double ⇒ JDouble }

  val TEMPERATURE = "temperature_by_sensor"
  val SENSORS = "sensors"
  val KEYSPACE = "journal"

  val sensors: List[JLong] = List(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l)

  val measureSize = 100

  /**
   *
   * Row size = {{sensors.size}}
   * All measures by sensor are grouped in single row
   *
   *     +------------------------------+-------------------------------+
   *     |2015Apr12-12:10:01:temperature|2015Apr12-12:10:02:temperature |
   * +---+------------------------------+-------------------------------+
   * | 1 |32.1                          |35.9                           |
   * +---+------------------------------+-------------------------------+
   *
   *     +------------------------------+------------------------------+
   *     |2015Apr12-12:10:01:temperature|2015Apr12-12:10:05:temperature|
   * +---+------------------------------+------------------------------+
   * | 2 |37.0                          |36.0                          |
   * +---+------------------------------+------------------------------+
   *
   * Partition key - sensor
   * Clustering key - event_time
   *
   * Optimal structure for filter/search temperature by sensor and event_time
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

  val queryOps = new QueryOptions().setFetchSize(1000).setConsistencyLevel(ConsistencyLevel.ONE)

  override protected def beforeAll(): Unit = {
    EmbeddedCassandra.start

    val clusterBuilder = com.datastax.driver.core.Cluster.builder
      .addContactPointsWithPorts(cassandraHost)
      .withQueryOptions(queryOps)

    val cluster = clusterBuilder.build
    val session = cluster.connect()

    executeWithRetry(3) {
      session.execute(createKeyspace)
    }
    session.execute(createReportedSensors)
    session.execute(createTableTemperatureBySensor)

    val sensorsInsert = sensors.map(id ⇒ session.prepare(writeSensors).bind(id: JLong, s"Description for sensor $id")).asJava

    def temperature(): JDouble = ThreadLocalRandom.current().nextDouble(40.9d)

    val metersData = (1 to measureSize).flatMap { i ⇒
      val ts = System.currentTimeMillis
      sensors.map(session.prepare(writeTemperature).bind(_: JLong, ts: JLong, temperature()))
    }.asJava

    session.execute(new BatchStatement().addAll(sensorsInsert))
    session.execute(new BatchStatement().addAll(metersData))

    session.close
    cluster.close

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandra.clean()
    super.afterAll()
  }
}