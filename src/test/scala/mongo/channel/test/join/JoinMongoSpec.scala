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
import java.util.concurrent.atomic.AtomicLong

import com.mongodb.DBObject

import mongo.channel.test.{ MongoDbEnviroment, MongoIntegrationEnv }
import org.specs2.mutable.Specification
import rx.lang.scala.Subscriber
import rx.lang.scala.schedulers.ExecutionContextScheduler

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.io

class JoinMongoSpec extends Specification {
  import MongoIntegrationEnv._
  import join.Join
  import join.mongo.{ MongoProcess, MongoObservable }
  import mongo._
  import dsl.mongo._

  "Join with MongoProcess" in new MongoDbEnviroment {
    initMongo

    val buffer = mutable.Buffer.empty[String]
    val SinkBuffer = io.fillBuffer(buffer)
    implicit val c = client

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(outer: DBObject) = for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q

    val joinQuery = Join[MongoProcess].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"PK:${l.get("index")} - [FK:${r.get("lang")} - ${r.get("name")}]"
    }

    val p = for {
      joinLine ← eval(Task.now(client)) through joinQuery.out
      _ ← joinLine to SinkBuffer
    } yield ()

    p.runLog.run
    buffer.size === MongoIntegrationEnv.programmersSize
  }

  "Join with MongoObservable" in new MongoDbEnviroment {
    initMongo

    implicit val c = client

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val query = Join[MongoObservable].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"PK:${l.get("index")} - [FK:${r.get("lang")} - ${r.get("name")}]"
    }

    val pageSize = 7
    val count = new CountDownLatch(1)
    val responses = new AtomicLong(0)

    val S = new Subscriber[String] {
      override def onStart() = request(1)
      override def onNext(n: String) = {
        logger.info(n)
        if (responses.getAndIncrement() % pageSize == 0) {
          logger.info(s"★ ★ ★ Fetched page:[$pageSize] ★ ★ ★ ")
          request(pageSize)
        }
      }
      override def onError(e: Throwable) = {
        logger.info(s"OnError: ${e.getMessage}")
        count.countDown()
      }
      override def onCompleted() = {
        logger.info("Interaction has been completed")
        count.countDown()
      }
    }

    query
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(S)

    count.await()
    responses.get === MongoIntegrationEnv.programmersSize
  }
}