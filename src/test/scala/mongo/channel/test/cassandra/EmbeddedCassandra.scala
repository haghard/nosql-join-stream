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
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

object EmbeddedCassandra {

  def start(): Unit = {
    val f = new File(getClass.getClassLoader.getResource("test-cassandra.yaml").getPath) //cassandra_network_strategy.yaml
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(f, "./cas-tmp", 60000)
  }

  def clean(remainingAttempts: Int = 3): Unit = try {
    println("Cleaning embedded Cassandra ...")
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    println("Cleaned embedded Cassandra")
  } catch {
    case e: Exception if remainingAttempts > 0 ⇒
      val delay = 20000
      println(s"Cleaning embedded Cassandra failed: ${e.getMessage}. Attempting again after $delay ms ...")
      Thread.sleep(delay)
      clean(remainingAttempts - 1)
    case e: Exception ⇒
      println(s"Cleaning embedded Cassandra failed. Giving up ...")
      throw e
  }
}
