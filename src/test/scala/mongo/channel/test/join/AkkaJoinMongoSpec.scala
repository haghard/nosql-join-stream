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

import join.Joiner.AkkaConcurrentAttributes
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
import scala.concurrent.duration._
import akka.stream.scaladsl.Sink
import scalaz.{ -\/, \/- }

class AkkaJoinMongoSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val logger = Logger.getLogger("akka-join-stream")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val PkLimit = 5
  val qLang = for { q ← "index" $gte 0 $lt PkLimit } yield q

  def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

  def folder = { (acc: List[String], cur: String) ⇒ acc :+ cur }

  def cmb: (DBObject, DBObject) ⇒ String =
    (outer, inner) ⇒
      s"""[PK:${outer.get("index")}] - [FK:${inner.get("lang")} - ${inner.get("name")}]"""

  "MongoJoin with Akka Streams" in new MongoDbEnviroment {
    initMongo
    implicit val c = client
    val materializer = ActorMaterializer(
      ActorMaterializerSettings(system)
        .withDispatcher("akka.join-dispatcher")
    )

    implicit val Attributes = -\/(system)

    val joinSource =
      Join[MongoAkkaStream]
        .join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val futureSeq = joinSource
      .runWith(Sink.fold(List.empty[String])(folder))(materializer)

    val seqRes = Await.result(futureSeq, 5 seconds)

    logger.info("Seq: " + seqRes)

    seqRes.size === MongoIntegrationEnv.programmersSize
  }

  "MongoJoinPar with Akka Streams" in new MongoDbEnviroment {
    initMongo
    import scalaz.std.AllInstances._

    implicit val c = client

    val settings = ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher")
    implicit val Attributes = \/-(AkkaConcurrentAttributes(settings, system, scalaz.Monoid[String]))

    val parSource = Join[MongoAkkaStream].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val futurePar = parSource
      .runWith(Sink.fold(List.empty[String])(folder))(ActorMaterializer(settings)(system))

    val parRes = Await.result(futurePar, 5 seconds)

    logger.info("Par: " + parRes)
    parRes.size === PkLimit
  }
}