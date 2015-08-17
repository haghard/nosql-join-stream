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

package object mongo {

  case class MongoReadSettings(q: com.mongodb.DBObject, sort: Option[com.mongodb.DBObject] = None,
                               limit: Option[Int] = None, skip: Option[Int] = None)

  trait MongoProcess extends StorageModule {
    override type Client = com.mongodb.MongoClient
    override type Record = com.mongodb.DBObject
    override type ReadSettings = MongoReadSettings
    override type Cursor = com.mongodb.Cursor
    override type Stream[Out] = _root_.mongo.channel.DBChannel[Client, Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait MongoObservable extends StorageModule {
    override type Client = com.mongodb.MongoClient
    override type Record = com.mongodb.DBObject
    override type ReadSettings = MongoReadSettings
    override type Cursor = com.mongodb.Cursor
    override type Stream[Out] = rx.lang.scala.Observable[Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait MongoAkkaStream extends StorageModule {
    override type Client = com.mongodb.MongoClient
    override type Record = com.mongodb.DBObject
    override type ReadSettings = MongoReadSettings
    override type Cursor = com.mongodb.Cursor
    override type Stream[Out] = akka.stream.scaladsl.Source[Out, Unit]
    override type Context = akka.actor.ActorSystem
  }
}
