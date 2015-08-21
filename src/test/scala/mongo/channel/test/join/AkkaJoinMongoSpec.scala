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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

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
import akka.stream.{Supervision, ActorMaterializerSettings, ActorMaterializer}
import mongo.channel.test.{ MongoIntegrationEnv, MongoDbEnviroment }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import akka.stream.scaladsl.Sink
import scala.util.{Failure, Success}
import scalaz.{ -\/, \/- }

class AkkaJoinMongoSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val logger = Logger.getLogger("akka-join-stream")
  implicit val dispatcher = system.dispatchers.lookup("akka.join-dispatcher")

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
    val latch = new CountDownLatch(1)
    val resRef = new AtomicReference(List.empty[String])
    implicit val c = client
    val decider: Supervision.Decider = {
      case _ ⇒ Supervision.Stop
    }
    val materializer = ActorMaterializer(
      ActorMaterializerSettings(system)
        .withDispatcher("akka.join-dispatcher")
        .withSupervisionStrategy(decider)
    )

    implicit val Attributes = -\/(system)

    val joinSource =
      Join[MongoAkkaStream]
        .join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val futureSeq = joinSource
      .runWith(Sink.fold(List.empty[String])(folder))(materializer)

    futureSeq.onComplete {
      case Success(r) ⇒
        resRef.set(r)
        latch.countDown()
      case Failure(ex) ⇒
        logger.info("***** Sequentual akka join error:" + ex.getMessage)
        latch.countDown()
    }

    latch.await()
    logger.info("Seq: " + resRef.get())
    resRef.get().size mustBe MongoIntegrationEnv.programmersSize
  }

  "MongoJoinPar with Akka Streams" in new MongoDbEnviroment {
    initMongo
    val latch = new CountDownLatch(1)
    val resRef = new AtomicReference(List.empty[String])
    import scalaz.std.AllInstances._

    implicit val c = client

    val settings = ActorMaterializerSettings(system).withDispatcher("akka.join-dispatcher")
    implicit val Attributes = \/-(AkkaConcurrentAttributes(settings, system, 4, scalaz.Monoid[String]))

    val parSource = Join[MongoAkkaStream].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val futurePar = parSource
      .runWith(Sink.fold(List.empty[String])(folder))(ActorMaterializer(settings)(system))

    futurePar.onComplete {
      case Success(r) ⇒
        resRef.set(r)
        latch.countDown()
      case Failure(ex) ⇒
        logger.info("*****Parallel akka join error:" + ex.getMessage)
        latch.countDown()
    }

    latch.await()
    logger.info("Par: " + resRef.get)
    resRef.get.size mustBe PkLimit
  }
}