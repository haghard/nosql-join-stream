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

import java.text.MessageFormat
import java.util.concurrent.ExecutorService

import dsl.QFree
import dsl.cassandra.CassandraQueryInterpreter
import dsl.mongo.MongoQueryInterpreter
import join.StorageModule
import mongo.channel.DBChannel
import org.apache.log4j.Logger
import com.mongodb.{ DBObject, MongoClient }
import com.datastax.driver.core.{ Session, Row, Cluster }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Subscriber, Observable }

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import scalaz.concurrent.Task
import scalaz.stream.{ Cause, io }
import scalaz.stream.Process
import scalaz.syntax.id._

package object storage {

  private def scheduler(exec: ExecutorService) =
    ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

  private trait Fetcher[T <: StorageModule, E] {
    def interpreter: QueryInterpreter[T]
    def resource: String
    def collection: String
    def c: T#Client
    def q: QFree[T#ReadSettings]
    def log: Logger
    def subscriber: Subscriber[E]
    def cursor: Option[T#Cursor]

    protected def extract(c: T#Cursor): E = {
      val r = c.next().asInstanceOf[E]
      log.info(s"fetch $r")
      r
    }

    def producer(): Long ⇒ Unit
  }

  trait QueryInterpreter[T <: StorageModule] {
    /**
     *
     */
    def interpret(q: QFree[T#ReadSettings]): T#ReadSettings
  }

  object QueryInterpreter {
    import scalaz.Free.runFC
    import join.mongo.{ MongoObservable, MongoProcess, MongoReadSettings }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraReadSettings }

    implicit object MongoPQueryInterpreter extends QueryInterpreter[MongoProcess] {
      override def interpret(q: QFree[MongoReadSettings]): MongoReadSettings =
        runFC(q)(MongoQueryInterpreter).run(MongoReadSettings(new com.mongodb.BasicDBObject))._1
    }

    implicit object MongoOQueryInterpreter extends QueryInterpreter[MongoObservable] {
      override def interpret(q: QFree[MongoReadSettings]): MongoReadSettings =
        runFC(q)(MongoQueryInterpreter).run(MongoReadSettings(new com.mongodb.BasicDBObject))._1
    }

    implicit object CassandraPQueryInterpreter extends QueryInterpreter[CassandraProcess] {
      override def interpret(q: QFree[CassandraReadSettings]): CassandraReadSettings =
        runFC(q)(CassandraQueryInterpreter).run(CassandraReadSettings(""))._1
    }

    implicit object CassandraOQueryInterpreter extends QueryInterpreter[CassandraObservable] {
      override def interpret(q: QFree[CassandraReadSettings]): CassandraReadSettings =
        runFC(q)(CassandraQueryInterpreter).run(CassandraReadSettings(""))._1
    }
  }

  abstract class Storage[T <: StorageModule] {
    /**
     *
     *
     */
    def outerR(q: QFree[T#ReadSettings], collection: String, resource: String,
               log: Logger, exec: ExecutorService): T#Client ⇒ T#Stream[T#Record]
    /**
     *
     *
     */
    def innerR(r: T#Record ⇒ QFree[T#ReadSettings], collection: String, resource: String,
               log: Logger, exec: ExecutorService): T#Client ⇒ (T#Record ⇒ T#Stream[T#Record])
  }

  object Storage {
    import join.mongo.{ MongoObservable, MongoProcess, MongoReadSettings }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraReadSettings }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      private def mongo[T](q: QFree[MongoReadSettings], client: MongoClient, c: String, resource: String,
                           logger: Logger, exec: ExecutorService): Process[Task, T] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[MongoProcess]].interpret(q)
          val coll = client.getDB(resource).getCollection(c)
          val cursor = coll.find(qs.q)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s"Query-settings: Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
          cursor
        })(c ⇒ Task.delay(c.close())) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }

      override def outerR(qs: QFree[MongoProcess#ReadSettings], collection: String, resName: String,
                          logger: Logger, exec: ExecutorService): (MongoClient) ⇒ DBChannel[MongoClient, DBObject] =
        client ⇒
          DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task.now { client: MongoProcess#Client ⇒
            Task(mongo[MongoProcess#Record](qs, client, collection, resName, logger, exec))
          }))

      override def innerR(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ DBChannel[MongoClient, DBObject] = {
        client ⇒
          parent ⇒
            DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task.now { client: MongoProcess#Client ⇒
              Task(mongo[MongoProcess#Record](r(parent), client, c, resource, log, exec))
            }))
      }
    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      private trait MongoFetcher[E] extends Fetcher[MongoObservable, E] {
        lazy val cursor: Option[MongoObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(q)
            val cursor = c.getDB(resource).getCollection(collection).find(qs.q)
            cursor |> { c ⇒
              qs.sort.foreach(c.sort)
              qs.skip.foreach(c.skip)
              qs.limit.foreach(c.limit)
            }
            log.debug(s"Query-settings: Sort:[${qs.sort}] Skip:[${qs.skip}] Limit:[${qs.limit}] Query:[${qs.q}]")
            cursor
          }
        } recover {
          case e: Throwable ⇒
            subscriber.onError(e)
            None
        }).get

        override def producer(): (Long) ⇒ Unit =
          n ⇒ go(n)

        @tailrec private def go(n: Long): Unit = {
          log.info(s"request $n")
          if (n > 0) {
            if (cursor.exists(_.hasNext())) {
              subscriber.onNext(extract(cursor.get))
              go(n - 1)
            } else subscriber.onCompleted()
          }
        }
      }

      private def mongo[A](q0: QFree[MongoReadSettings], col: String, resource0: String,
                           log0: Logger, client: MongoObservable#Client, exec: ExecutorService) =
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(
            new MongoFetcher[A] {
              override def subscriber = subscriber0
              override def q: QFree[MongoReadSettings] = q0
              override def log = log0
              override def resource = resource0
              override def collection = col
              override def c = client
              override def interpreter = implicitly[QueryInterpreter[MongoObservable]]
            }.producer)
        }.subscribeOn(scheduler(exec))

      override def outerR(q: QFree[MongoObservable#ReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ Observable[DBObject] = {
        client ⇒
          mongo[MongoObservable#Record](q, c, resource, log, client, exec)
      }

      override def innerR(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ Observable[DBObject] =
        client ⇒
          parent ⇒
            mongo[MongoObservable#Record](r(parent), c, resource, log, client, exec)
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      private[this] def cassandra[T](q: QFree[CassandraReadSettings], session: Session, c: String,
                                     logger: Logger, exec: ExecutorService): Process[Task, T] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[CassandraProcess]].interpret(q)
          val query = MessageFormat.format(qs.q, c)
          logger.debug(s"Query-settings: Query:[ $query ] Param: [ ${qs.v} ]")
          qs.v.fold(session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind()).iterator) { r ⇒
            session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind(r.v)).iterator
          }
        })(c ⇒ Task.delay(session.close())) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }

      override def outerR(q: QFree[CassandraReadSettings], c: String, r: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task.now { client: CassandraProcess#Client ⇒
            Task(cassandra[CassandraProcess#Record](q, client.connect(r), c, log, exec))
          }))
      }

      override def innerR(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          parent ⇒
            DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task.now { client: CassandraProcess#Client ⇒
              Task(cassandra[CassandraProcess#Record](r(parent), client.connect(res), c, log, exec))
            }))
      }
    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      private trait CassandraFetcher[E] extends Fetcher[CassandraObservable, E] {
        private val defaultPageSize = 8
        lazy val cursor: Option[CassandraObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(q)
            val query = MessageFormat.format(qs.q, collection)
            val session = c.connect(resource)
            log.debug(s"Query-settings: Query:[ $query ] Param: [ ${qs.v} ]")
            qs.v.fold(session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind()).iterator()) { r ⇒
              session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind(r.v)).iterator()
            }
          }
        } recover {
          case e: Throwable ⇒
            subscriber.onError(e)
            None
        }).get

        @tailrec private def fetch(n: Int, i: Int, c: CassandraObservable#Cursor): Unit = {
          if (i < n && c.hasNext && !subscriber.isUnsubscribed) {
            val r = extract(c)
            subscriber.onNext(r)
            fetch(n, i + 1, c)
          }
        }

        override def producer: (Long) ⇒ Unit =
          n ⇒ {
            val intN = if (n >= defaultPageSize) defaultPageSize else n.toInt
            if (cursor.isDefined) {
              if (cursor.get.hasNext) fetch(intN, 0, cursor.get)
              else subscriber.onCompleted()
            }
          }
      }

      private def cassandra[A](q0: QFree[CassandraReadSettings], client0: CassandraObservable#Client,
                               c0: String, resource0: String, log0: Logger, exec: ExecutorService): Observable[A] = {
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(new CassandraFetcher[A] {
            override def subscriber = subscriber0
            override def q = q0
            override def log = log0
            override def resource = resource0
            override def collection = c0
            override def c = client0
            override def interpreter = implicitly[QueryInterpreter[CassandraObservable]]
          }.producer)
        }.subscribeOn(scheduler(exec))
      }

      override def outerR(q: QFree[CassandraReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ Observable[Row] = {
        client ⇒ cassandra[CassandraObservable#Record](q, client, c, resource, log, exec)
      }

      override def innerR(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ Observable[Row] = {
        client ⇒
          parent ⇒ cassandra[CassandraObservable#Record](r(parent), client, c, res, log, exec)
      }
    }

    /**
     *
     * @tparam T
     * @return
     */
    def apply[T <: StorageModule: Storage]: Storage[T] = implicitly[Storage[T]]
  }
}
