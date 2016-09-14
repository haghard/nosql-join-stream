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

import java.util.concurrent.{ TimeUnit, CountDownLatch }
import java.util.concurrent.atomic.AtomicReference

import mongo._
import join.Join
import dsl.mongo._
import akka.testkit.TestKit
import com.mongodb.DBObject
import akka.actor.ActorSystem
import join.mongo.MongoSource
import mongo.channel.test.MongoIntegrationEnv._
import akka.stream.{ Supervision, ActorMaterializerSettings, ActorMaterializer }
import mongo.channel.test.{ MongoIntegrationEnv, MongoDbEnviroment }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import scala.util.{ Failure, Success }

class AkkaJoinMongoSpec extends TestKit(ActorSystem("akka-join-stream")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  //val logger = org.slf4j.LoggerFactory.getLogger("akka-join-stream")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val PkLimit = 5
  val qLang = for { q ← "index" $gte 0 $lt PkLimit } yield q

  def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

  def cmb: (MongoSource#Record, MongoSource#Record) ⇒ String =
    (outer, inner) ⇒
      s"""[PK:${outer.get("index")}] - [FK:${inner.get("lang")} - ${inner.get("name")}]"""

  def decider(c: MongoSource#Client): Supervision.Decider = {
    case _ ⇒
      c.close()
      Supervision.Stop
  }

  val dName = "akka.join-dispatcher"
  implicit val dispatcher = system.dispatchers.lookup(dName)

  "MongoJoin with Akka Streams" in new MongoDbEnviroment {
    initMongo
    implicit val c = client
    val settings = ActorMaterializerSettings(system)
      .withInputBuffer(32, 64)
      .withDispatcher(dName)
      .withSupervisionStrategy(decider(c))
    implicit val Mat = ActorMaterializer(settings)

    val latch = new CountDownLatch(1)
    val resRef = new AtomicReference(List.empty[String])

    val joinSource = Join[MongoSource].inner(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val futureSeq = joinSource.source
      .runFold(List.empty[String]) { (acc, cur) ⇒ cur :: acc }

    futureSeq.onComplete {
      case Success(r) ⇒
        resRef.set(r)
        latch.countDown()
      case Failure(ex) ⇒
        logger.info("★ ★ ★ MongoAkkaStream join has been competed with error {}", ex)
        latch.countDown()
    }

    latch.await(15, TimeUnit.SECONDS) mustBe true
    logger.info("Seq: {}", resRef.get())
    c.close()
    resRef.get().size mustBe MongoIntegrationEnv.programmersSize
  }
}