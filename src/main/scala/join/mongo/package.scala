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

  case class MongoReadSettings(query: com.mongodb.DBObject, sort: Option[com.mongodb.DBObject] = None,
                               limit: Option[Int] = None, skip: Option[Int] = None)

  abstract sealed trait Mongo extends StorageModule {
    override type Client = com.mongodb.MongoClient
    override type Session = com.mongodb.DB
    override type Record = com.mongodb.DBObject
    override type QueryAttributes = MongoReadSettings
    override type Cursor = com.mongodb.Cursor
  }

  trait MongoProcess extends Mongo {
    override type Stream[Out] = _root_.mongo.channel.ScalazStreamsOps[Session, Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait MongoObservable extends Mongo {
    override type Stream[Out] = rx.lang.scala.Observable[Out]
    override type Context = java.util.concurrent.ExecutorService
  }

  trait MongoObsCursorError extends MongoObservable
  trait MongoObsFetchError extends MongoObservable

  trait MongoSource extends Mongo {
    override type Stream[Out] = _root_.mongo.channel.AkkaStreamsOps[Out, akka.NotUsed]
    override type Context = scala.concurrent.ExecutionContext
  }
}