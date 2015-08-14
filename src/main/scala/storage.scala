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
    def coll: String
    def client: T#Client
    def q: QFree[T#ReadSettings]
    def log: Logger
    def subscriber: Subscriber[E]
    def cursor: Option[T#Cursor]

    protected def cast(c: T#Cursor): E = {
      val r = c.next().asInstanceOf[E]
      log.debug(s"fetch $r")
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
    def outer(q: QFree[T#ReadSettings], collection: String, resource: String,
              log: Logger, exec: ExecutorService): T#Client ⇒ T#Stream[T#Record]
    /**
     *
     *
     */
    def inner(r: T#Record ⇒ QFree[T#ReadSettings], collection: String, resource: String,
              log: Logger, exec: ExecutorService): T#Client ⇒ (T#Record ⇒ T#Stream[T#Record])
  }

  object Storage {
    import join.mongo.{ MongoObservable, MongoProcess, MongoReadSettings }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraReadSettings }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      private def mongoR[T](query: QFree[MongoReadSettings], client: MongoClient, coll: String,
                            dbName: String, logger: Logger): Process[Task, T] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[MongoProcess]].interpret(query)
          val cursor = client.getDB(dbName).getCollection(coll).find(qs.q)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s"★ ★ ★ Create Process-Fetcher for query from $coll Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
          cursor
        })(c ⇒ Task.delay(c.close())) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }
        }

      override def outer(qs: QFree[MongoProcess#ReadSettings], collection: String, resName: String,
                          logger: Logger, exec: ExecutorService): (MongoClient) ⇒ DBChannel[MongoClient, DBObject] =
        client ⇒
          DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
            Task.delay(mongoR[MongoProcess#Record](qs, client, collection, resName, logger))
          }(exec)))

      override def inner(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ DBChannel[MongoClient, DBObject] = {
        client ⇒
          outer ⇒
            DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
              Task.delay(mongoR[MongoProcess#Record](r(outer), client, c, resource, log))
            }(exec)))
      }
    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      private trait MongoFetcher[E] extends Fetcher[MongoObservable, E] {
        lazy val cursor: Option[MongoObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(q)
            val cursor = client.getDB(resource).getCollection(coll).find(qs.q)
            cursor |> { c ⇒
              qs.sort.foreach(c.sort)
              qs.skip.foreach(c.skip)
              qs.limit.foreach(c.limit)
            }
            log.debug(s"★ ★ ★ Create Observable-Fetcher for query from $coll Sort:[${qs.sort}] Skip:[${qs.skip}] Limit:[${qs.limit}] Query:[${qs.q}] ★ ★ ★")
            cursor
          }
        } recover {
          case e: Throwable ⇒
            subscriber.onError(e)
            None
        }).get

        override val producer: Long ⇒ Unit =
          n ⇒
            try go(n)
            catch { case e: Exception =>
              subscriber.onError(e)
              cursor.foreach(_.close())
            }

        @tailrec private def go(n: Long): Unit =
          if (n > 0) {
            if (cursor.exists(_.hasNext())) {
              subscriber.onNext(cast(cursor.get))
              go(n - 1)
            } else {
              subscriber.onCompleted()
              cursor.foreach(_.close())
            }
          }
      }

      private def mongoR[A](q0: QFree[MongoReadSettings], coll0: String, resource0: String,
                           log0: Logger, client0: MongoObservable#Client, exec: ExecutorService) =
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(
            new MongoFetcher[A] {
              override def subscriber = subscriber0
              override def q: QFree[MongoReadSettings] = q0
              override def log = log0
              override def resource = resource0
              override def coll = coll0
              override def client = client0
              override def interpreter = implicitly[QueryInterpreter[MongoObservable]]
            }.producer)
        }.subscribeOn(scheduler(exec))

      override def outer(q: QFree[MongoObservable#ReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ Observable[DBObject] = {
        client ⇒
          mongoR[MongoObservable#Record](q, c, resource, log, client, exec)
      }

      override def inner(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ Observable[DBObject] =
        client ⇒
          outer ⇒
            mongoR[MongoObservable#Record](r(outer), c, resource, log, client, exec)
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      private def cassandraR[T](q: QFree[CassandraReadSettings], session: Session,
                                c: String, logger: Logger): Process[Task, T] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[CassandraProcess]].interpret(q)
          val query = MessageFormat.format(qs.q, c)
          logger.debug(s"★ ★ ★ Create Process-Fetcher for query: Query:[ $query ] Param: [ ${qs.v} ] ★ ★ ★")
          qs.v.fold(session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind()).iterator) { r ⇒
            session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind(r.v)).iterator
          }
        })(c ⇒ Task.delay(session.close())) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }
        }

      override def outer(q: QFree[CassandraReadSettings], c: String, r: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
            Task.delay(cassandraR[CassandraProcess#Record](q, client.connect(r), c, log))
          }(exec)))
      }

      override def inner(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          outer ⇒
            DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
              Task.delay(cassandraR[CassandraProcess#Record](r(outer), client.connect(res), c, log))
            }(exec)))
      }
    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      private trait CassandraFetcher[E] extends Fetcher[CassandraObservable, E] {
        private val defaultPageSize = 8
        lazy val cursor: Option[CassandraObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(q)
            val query = MessageFormat.format(qs.q, coll)
            val session = client.connect(resource)
            log.debug(s"★ ★ ★ Create Observable-Fetcher for query: Query:[ $query ] Param: [ ${qs.v} ] ★ ★ ★")
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
            val r = cast(c)
            subscriber.onNext(r)
            fetch(n, i + 1, c)
          }
        }

        override val producer: (Long) ⇒ Unit =
          n ⇒ {
            val intN = if (n >= defaultPageSize) defaultPageSize else n.toInt
            if (cursor.isDefined) {
              if (cursor.get.hasNext) fetch(intN, 0, cursor.get)
              else subscriber.onCompleted()
            }
          }
      }

      private def cassandraR[A](q0: QFree[CassandraReadSettings], client0: CassandraObservable#Client,
                                coll0: String, resource0: String, log0: Logger, exec: ExecutorService): Observable[A] = {
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(new CassandraFetcher[A] {
            override def subscriber = subscriber0
            override def q = q0
            override def log = log0
            override def resource = resource0
            override def coll = coll0
            override def client = client0
            override def interpreter = implicitly[QueryInterpreter[CassandraObservable]]
          }.producer)
        }.subscribeOn(scheduler(exec))
      }

      override def outer(q: QFree[CassandraReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ Observable[Row] = {
        client ⇒ cassandraR[CassandraObservable#Record](q, client, c, resource, log, exec)
      }

      override def inner(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ Observable[Row] = {
        client ⇒
          parent ⇒ cassandraR[CassandraObservable#Record](r(parent), client, c, res, log, exec)
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