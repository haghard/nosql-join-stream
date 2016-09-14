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

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import mongo.NamedThreadFactory
import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }
import scala.collection.JavaConverters._

trait CassandraEnviroment {

  //val logger = org.slf4j.LoggerFactory.getLogger("cassandra")
  val logger = org.slf4j.LoggerFactory.getLogger(classOf[CassandraEnviroment])

  val createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS journal
      WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' }
    """

  implicit val executor: java.util.concurrent.ExecutorService =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new NamedThreadFactory("cassandra-worker"))

  val cassandraHost = List(new InetSocketAddress("127.0.0.1", 9142)).asJava

  def executeWithRetry[T](n: Int)(f: ⇒ T) = retry(n)(f)

  @tailrec private def retry[T](n: Int)(f: ⇒ T): T =
    Try(f) match {
      case Success(x) ⇒ x
      case _ if n > 1 ⇒ retry(n - 1)(f)
      case Failure(e) ⇒ throw e
    }
}
