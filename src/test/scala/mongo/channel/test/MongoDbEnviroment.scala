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

import _root_.join.StorageModule
import com.mongodb.MongoClient
import de.bwaldvogel.mongo.MongoServer
import org.specs2.mutable.After

trait MongoDbEnviroment extends After {
  import MongoIntegrationEnv._

  type M <: StorageModule

  val logger = org.slf4j.LoggerFactory.getLogger(classOf[MongoDbEnviroment])
  var client: MongoClient = _
  var server: MongoServer = _

  def initMongo = {
    val r = prepareMockMongo()
    client = r._1
    server = r._2
  }

  override def after = {
    client.close
    server.shutdown
    logger.info("Mongo client and server have been closed")
  }
}
