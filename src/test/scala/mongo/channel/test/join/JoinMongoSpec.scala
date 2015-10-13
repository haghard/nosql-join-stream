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
import java.util.concurrent.atomic.AtomicLong
import mongo.channel.test.{ MongoDbEnviroment, MongoIntegrationEnv }
import org.scalatest.concurrent.ScalaFutures
import org.specs2.mutable.Specification
import rx.lang.scala.Subscriber
import rx.lang.scala.schedulers.ExecutionContextScheduler

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.io

class JoinMongoSpec extends Specification with ScalaFutures {
  import MongoIntegrationEnv._
  import join.Join
  import mongo._
  import dsl.mongo._
  import join.mongo.{ MongoProcess, MongoObservable, MongoObsCursorError, MongoObsFetchError }

  val pageSize = 7

  val qLang = for { q ← "index" $gte 0 $lte 5 } yield {
    q
  }

  def count = new CountDownLatch(1)
  def responses = new AtomicLong(0)

  "Join with MongoProcess" in new MongoDbEnviroment {
    initMongo

    type Module = MongoProcess

    def qProg(outer: Module#Record) = for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q

    val cmd: (Module#Record, Module#Record) ⇒ String =
      (outer, inner) ⇒
        s"PK:${outer.get("index")} - [FK:${inner.get("lang")} - ${inner.get("name")}]"

    val buffer = mutable.Buffer.empty[String]
    val SinkBuffer = io.fillBuffer(buffer)
    implicit val c = client

    val joinQuery = Join[Module].left(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmd)

    val p = for {
      joinLine ← eval(Task.now(client)) through joinQuery.out
      _ ← joinLine to SinkBuffer
    } yield ()

    p.runLog.run
    buffer.size === MongoIntegrationEnv.programmersSize
  }

  "Join with MongoObservable" in new MongoDbEnviroment {
    initMongo

    type Module = MongoObservable

    def qProg(outer: Module#Record) =
      for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q

    val cmb: (Module#Record, Module#Record) ⇒ String =
      (outer, inner) ⇒
        s"PK:${outer.get("index")} - [FK:${inner.get("lang")} - ${inner.get("name")}]"

    val c0 = count
    val res = responses
    implicit val c = client

    val query = Join[Module].left(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmb)

    val S = new Subscriber[String] {
      override def onStart() = request(pageSize)
      override def onNext(n: String) = {
        logger.info(s"onNext: $n")
        if (res.getAndIncrement() % pageSize == 0) {
          logger.info(s"★ ★ ★ Fetched page:[$pageSize] ★ ★ ★ ")
          request(pageSize)
        }
      }
      override def onError(e: Throwable) = {
        logger.info(s"★ ★ ★  MongoObservable has been completed with error: ${e.getMessage}")
        c0.countDown()
      }
      override def onCompleted() = {
        logger.info("★ ★ ★   MongoObservable has been completed")
        c0.countDown()
      }
    }

    query
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(S)

    c0.await()
    res.get === MongoIntegrationEnv.programmersSize
  }

  "Run Mongo Observable with MongoObsCursorError error" in new MongoDbEnviroment {
    initMongo
    type Module = MongoObsCursorError

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q

    def qProg(outer: Module#Record) =
      for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield {
        q
      }

    val cmd: (Module#Record, Module#Record) ⇒ String =
      (outer, inner) ⇒
        s"PK:${outer.get("index")} - [FK:${inner.get("lang")} - ${inner.get("name")}]"

    val c0 = count
    val res = responses
    implicit val c = client

    val query = Join[Module].left(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmd)

    val S = new Subscriber[String] {
      override def onStart() = request(pageSize)

      override def onNext(n: String) = {
        logger.info(s"onNext: $n")
        if (responses.getAndIncrement() % pageSize == 0) {
          request(pageSize)
        }
      }

      override def onError(e: Throwable) = {
        logger.info(s"★ ★ ★  MongoObsCursorError has been completed with error: ${e.getMessage}")
        c0.countDown()
      }

      override def onCompleted() =
        logger.info("★ ★ ★  MongoObsCursorError has been completed")
    }

    query
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(S)

    c0.await(5, TimeUnit.SECONDS) mustEqual true
  }

  "Run Mongo Observable with MongoObsFetchError" in new MongoDbEnviroment {
    initMongo
    type Module = MongoObsFetchError

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q

    def qProg(outer: Module#Record) = for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q

    val cmd: (Module#Record, Module#Record) ⇒ String =
      (outer, inner) ⇒
        s"PK:${outer.get("index")} - [FK:${inner.get("lang")} - ${inner.get("name")}]"

    val c0 = count
    val res = responses
    implicit val c = client

    val query = Join[Module].left(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)(cmd)

    val S = new Subscriber[String] {
      override def onStart() = request(pageSize)
      override def onNext(n: String) = {
        logger.info(s"onNext: $n")
        if (responses.getAndIncrement() % pageSize == 0) {
          request(pageSize)
        }
      }

      override def onError(e: Throwable) = {
        logger.info(s"★ ★ ★  MongoObsFetchError has been completed with error: ${e.getMessage}")
        c0.countDown()
      }

      override def onCompleted() =
        logger.info("★ ★ ★  MongoObsFetchError has been completed")
    }

    query
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(S)

    c0.await(5, TimeUnit.SECONDS) mustEqual true
  }
}