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

package mongo.channel.test.join

import mongo._
import join.Join
import dsl.mongo._
import akka.testkit.TestKit
import com.mongodb.DBObject
import akka.actor.ActorSystem
import org.apache.log4j.Logger
import join.mongo.MongoAkkaStream
import mongo.channel.test.MongoIntegrationEnv._
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import mongo.channel.test.{ MongoIntegrationEnv, MongoDbEnviroment }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.concurrent.Await

class AkkaJoinMongoSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val logger = Logger.getLogger("akka-join-stream")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system)
    .withDispatcher("akka.join-dispatcher"))

  "MongoJoin with Akka Streams" in new MongoDbEnviroment {
    initMongo

    implicit val c = client

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q

    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val joinSource = Join[MongoAkkaStream].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (outer, inner) ⇒
      s"""[PK:${outer.get("index")}] - [FK:${inner.get("lang")} - ${inner.get("name")}]"""
    }

    val folder = { (acc: List[String], cur: String) ⇒ logger.info(s"consume $cur"); acc :+ cur }

    val future = joinSource.runWith(akka.stream.scaladsl.Sink.fold(List.empty[String])(folder))

    import scala.concurrent.duration._
    Await.result(future, 5 seconds).size === MongoIntegrationEnv.programmersSize
  }
}