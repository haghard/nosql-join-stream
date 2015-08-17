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

package join

package object cassandra {
  import com.datastax.driver.core.ConsistencyLevel

  case class CassandraParamValue(name: String, v: AnyRef, clazz: Class[_])
  case class CassandraReadSettings(query: String, v: Option[CassandraParamValue] = None,
                                   consistencyLevel: ConsistencyLevel = ConsistencyLevel.ONE)

  trait CassandraObservable extends StorageModule {
    override type Client = com.datastax.driver.core.Cluster
    override type Record = com.datastax.driver.core.Row
    override type ReadSettings = CassandraReadSettings
    override type Cursor = java.util.Iterator[com.datastax.driver.core.Row]
    override type Stream[Out] = rx.lang.scala.Observable[Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait CassandraProcess extends StorageModule {
    override type Client = com.datastax.driver.core.Cluster
    override type Record = com.datastax.driver.core.Row
    override type ReadSettings = CassandraReadSettings
    override type Cursor = java.util.Iterator[com.datastax.driver.core.Row]
    override type Stream[Out] = _root_.mongo.channel.DBChannel[Client, Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait CassandraAkkaStream extends StorageModule {
    override type Client = com.datastax.driver.core.Cluster
    override type Record = com.datastax.driver.core.Row
    override type ReadSettings = CassandraReadSettings
    override type Cursor = java.util.Iterator[com.datastax.driver.core.Row]
    override type Stream[Out] = akka.stream.scaladsl.Source[Out, Unit]
    override type Context = akka.actor.ActorSystem
  }
}
