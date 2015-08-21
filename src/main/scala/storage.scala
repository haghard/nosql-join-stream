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
import akka.stream.scaladsl.Source
import dsl.QFree
import dsl.cassandra.CassandraQueryInterpreter
import dsl.mongo.MongoQueryInterpreter
import join.StorageModule
import join.cassandra.{CassandraProcess, CassandraAkkaStream}
import join.mongo.{MongoProcess, MongoAkkaStream}
import mongo.channel.DBChannel
import org.apache.log4j.Logger
import com.mongodb.{DBObject, MongoClient}
import com.datastax.driver.core.{ Session, Row, Cluster }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Subscriber, Observable }
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

  private[storage] trait Producer[Module <: StorageModule, E] {
    def interpreter: QueryInterpreter[Module]
    def resource: String

    def collection: String

    def client: Module#Client

    def settings: QFree[Module#QueryAttributes]
    def log: Logger
    def subscriber: Subscriber[E]

    def cursor: Option[Module#Cursor]

    protected def cast(c: Module#Cursor): E = {
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
    def interpret(q: QFree[T#QueryAttributes]): T#QueryAttributes
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

  private[storage] trait DbIterator[Module <: StorageModule] extends Iterator[Module#Record] {
    def logger: Logger
    def resource: String
    def collection: String
    def client: Module#Client

    def settings: QFree[Module#QueryAttributes]

    def cursor: Module#Cursor

    def attributes: Module#QueryAttributes

    override def hasNext: Boolean = {
      val r = cursor.hasNext
      if (!r) {
        logger.debug(s"The cursor for $attributes has been exhausted")
      }
      r
    }

    override def next(): Module#Record = cursor.next()
  }

  object DbIterator {

    case class CassandraIterator(settings: QFree[CassandraAkkaStream#QueryAttributes], client: CassandraAkkaStream#Client,
                                 resource: String, collection: String, logger: Logger) extends DbIterator[CassandraAkkaStream] {
      override val attributes = implicitly[QueryInterpreter[CassandraProcess]].interpret(settings)
      override val cursor = {
        val queryStr = MessageFormat.format(attributes.query, collection)
        val session = client.connect(resource)
        attributes.v.fold(session.execute(session.prepare(queryStr).setConsistencyLevel(attributes.consistencyLevel).bind()).iterator()) { r ⇒
          session.execute(session.prepare(queryStr).setConsistencyLevel(attributes.consistencyLevel).bind(r.v)).iterator()
        }
      }
    }

    case class MongoIterator(settings: QFree[MongoAkkaStream#QueryAttributes], client: MongoAkkaStream#Client,
                             resource: String, collection: String, logger: Logger) extends DbIterator[MongoAkkaStream] {
      override val attributes = implicitly[QueryInterpreter[MongoProcess]].interpret(settings)
      override val cursor = {
        val local = client.getDB(resource).getCollection(collection).find(attributes.query)
        local |> { c ⇒
          attributes.sort.foreach(c.sort)
          attributes.skip.foreach(c.skip)
          attributes.limit.foreach(c.limit)
        }
        local
      }
    }

    def mongo(settings: QFree[MongoAkkaStream#QueryAttributes], client: MongoAkkaStream#Client,
              resource: String, collection: String, logger: Logger) =
      MongoIterator(settings, client, resource, collection, logger)

    def cassandra(settings: QFree[CassandraAkkaStream#QueryAttributes], client: CassandraAkkaStream#Client,
                  resource: String, collection: String, logger: Logger) =
      CassandraIterator(settings, client, resource, collection, logger)
  }

  abstract class Storage[T <: StorageModule] {
    /**
     *
     *
     */
    def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
              log: Logger, ctx: T#Context): T#Client ⇒ T#Stream[T#Record]

    /**
     *
     *
     */
    def inner(r: T#Record ⇒ QFree[T#QueryAttributes], collection: String, resource: String,
              log: Logger, ctx: T#Context): T#Client ⇒ (T#Record ⇒ T#Stream[T#Record])
  }

  object Storage {

    import join.mongo.{MongoObservable, MongoProcess, MongoAkkaStream, MongoReadSettings}
    import join.cassandra.{CassandraObservable, CassandraProcess, CassandraAkkaStream, CassandraReadSettings}

    implicit object CassandraStorageAkkaStream extends Storage[CassandraAkkaStream] {

      override def outer(qs: QFree[CassandraAkkaStream#QueryAttributes], collection: String,
                         resource: String, log: Logger, ctx: CassandraAkkaStream#Context):
      (CassandraAkkaStream#Client) => CassandraAkkaStream#Stream[CassandraAkkaStream#Record] =
        client =>
          Source(() => DbIterator.cassandra(qs, client, resource, collection, log))

      override def inner(r: (Row) => QFree[CassandraAkkaStream#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: CassandraAkkaStream#Context):
      (CassandraAkkaStream#Client) => (CassandraAkkaStream#Record) => Source[CassandraAkkaStream#Record, Unit] = {
        client =>
          outer =>
            Source(() => DbIterator.cassandra(r(outer), client, resource, collection, log))
      }
    }

    implicit object MongoStorageAkkaStream extends Storage[MongoAkkaStream] {

      override def outer(qs: QFree[MongoAkkaStream#QueryAttributes], collection: String,
                         resource: String, log: Logger, ctx: MongoAkkaStream#Context):
                         (MongoAkkaStream#Client) => MongoAkkaStream#Stream[MongoAkkaStream#Record] =
        client =>
          Source(() => DbIterator.mongo(qs, client, resource, collection, log))

      override def inner(relation: (MongoAkkaStream#Record) => QFree[MongoAkkaStream#QueryAttributes], collection: String,
                         resource: String, log: Logger, ctx: MongoAkkaStream#Context):
        (MongoAkkaStream#Client) => (MongoAkkaStream#Record) => MongoAkkaStream#Stream[MongoAkkaStream#Record] =
          client =>
             outer =>
               Source(() => DbIterator.mongo(relation(outer), client, resource, collection, log))
    }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      private def mongoR[T](query: QFree[MongoReadSettings], client: MongoClient, coll: String,
                            dbName: String, logger: Logger): Process[Task, T] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[MongoProcess]].interpret(query)
          val cursor = client.getDB(dbName).getCollection(coll).find(qs.query)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s"★ ★ ★ Create Process-Fetcher for query from $coll Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.query} ]")
          cursor
        })(c ⇒ Task.delay(c.close())) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else {
              logger.info(s"The cursor has been exhausted")
              throw Cause.Terminated(Cause.End)
            }
          }
        }

      override def outer(qs: QFree[MongoProcess#QueryAttributes], collection: String, resource: String,
                          logger: Logger, ctx: MongoProcess#Context): (MongoClient) ⇒ DBChannel[MongoClient, DBObject] =
        client ⇒
          DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
            Task.delay(mongoR[MongoProcess#Record](qs, client, collection, resource, logger))
          }(ctx)))

      override def inner(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, ctx: MongoProcess#Context): (MongoClient) ⇒ (DBObject) ⇒ DBChannel[MongoClient, DBObject] = {
        client ⇒
          outer ⇒
            DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
              Task.delay(mongoR[MongoProcess#Record](r(outer), client, c, resource, log))
            }(ctx)))
      }
    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {

      private trait MongoProducer[E] extends Producer[MongoObservable, E] {
        lazy val cursor: Option[MongoObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(settings)
            val cursor = client.getDB(resource).getCollection(collection).find(qs.query)
            cursor |> { c ⇒
              qs.sort.foreach(c.sort)
              qs.skip.foreach(c.skip)
              qs.limit.foreach(c.limit)
            }
            log.debug(s"★ ★ ★ Create Observable-Fetcher for query from $collection Sort:[${qs.sort}] Skip:[${qs.skip}] Limit:[${qs.limit}] Query:[${qs.query}]")
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

        @tailrec private final def go(n: Long): Unit =
          if (n > 0) {
            if (cursor.exists(_.hasNext())) {
              subscriber.onNext(cast(cursor.get))
              go(n - 1)
            } else {
              subscriber.onCompleted()
              cursor.foreach(_.close())
              log.info(s"The cursor has been exhausted")
            }
          }
      }

      private def mongoR[A](q0: QFree[MongoReadSettings], coll0: String, resource0: String,
                           log0: Logger, client0: MongoObservable#Client, ctx: ExecutorService) =
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(
            new MongoProducer[A] {
              override def subscriber = subscriber0

              override def settings: QFree[MongoReadSettings] = q0
              override def log = log0
              override def resource = resource0

              override def collection = coll0
              override def client = client0
              override def interpreter = implicitly[QueryInterpreter[MongoObservable]]
            }.producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[MongoObservable#QueryAttributes], collection: String, resource: String,
                         logger: Logger, ctx: MongoObservable#Context): (MongoClient) ⇒ Observable[DBObject] = {
        client ⇒
          mongoR[MongoObservable#Record](q, collection, resource, logger, client, ctx)
      }

      override def inner(r: (DBObject) ⇒ QFree[MongoReadSettings], collection: String, resource: String,
                         logger: Logger, ctx: MongoObservable#Context): (MongoClient) ⇒ (DBObject) ⇒ Observable[DBObject] =
        client ⇒
          outer ⇒
            mongoR[MongoObservable#Record](r(outer), collection, resource, logger, client, ctx)
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      private def cassandraR[T](qr: QFree[CassandraReadSettings], session: Session,
                                collection: String, logger: Logger): Process[Task, T] =
        io.resource(Task.delay {
          val settings = implicitly[QueryInterpreter[CassandraProcess]].interpret(qr)
          val query = MessageFormat.format(settings.query, collection)
          logger.debug(s"★ ★ ★ Create Process-Fetcher for query: Query:[ $query ] Param: [ ${settings.v} ]")
          settings.v.fold(session.execute(session.prepare(query).setConsistencyLevel(settings.consistencyLevel).bind()).iterator) { r ⇒
            session.execute(session.prepare(query).setConsistencyLevel(settings.consistencyLevel).bind(r.v)).iterator
          }
        })(c ⇒ Task.delay(session.close())) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else {
              logger.info(s"The cursor has been exhausted")
              throw Cause.Terminated(Cause.End)
            }
          }
        }

      override def outer(qr: QFree[CassandraReadSettings], collection: String, resource: String,
                         logger: Logger, ctx: ExecutorService): (Cluster) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
            Task.delay(cassandraR[CassandraProcess#Record](qr, client.connect(resource), collection, logger))
          }(ctx)))
      }

      override def inner(r: (Row) ⇒ QFree[CassandraReadSettings], collection: String, resource: String,
                         logger: Logger, ctx: CassandraProcess#Context): (Cluster) ⇒ (Row) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          outer ⇒
            DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
              Task.delay(cassandraR[CassandraProcess#Record](r(outer), client.connect(resource), collection, logger))
            }(ctx)))
      }
    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {

      private trait CassandraProducer[E] extends Producer[CassandraObservable, E] {
        private val defaultPageSize = 8
        lazy val cursor: Option[CassandraObservable#Cursor] = (Try {
          Option {
            val qs = interpreter.interpret(settings)
            val query = MessageFormat.format(qs.query, collection)
            val session = client.connect(resource)
            log.debug(s"★ ★ ★ Create Observable-Fetcher for query: Query:[ $query ] Param: [ ${qs.v} ]")
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

      private def cassandraR[A](qs: QFree[CassandraReadSettings], client0: CassandraObservable#Client,
                                collection0: String, resourceName: String, logger: Logger,
                                ctx: ExecutorService): Observable[A] = {
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(new CassandraProducer[A] {
            override def subscriber = subscriber0

            override def settings = qs
            override def log = logger
            override def resource = resourceName

            override def collection = collection0
            override def client = client0
            override def interpreter = implicitly[QueryInterpreter[CassandraObservable]]
          }.producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(q: QFree[CassandraReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ Observable[Row] = {
        client ⇒ 
          cassandraR[CassandraObservable#Record](q, client, c, resource, log, exec)
      }

      override def inner(relation: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, ctx: ExecutorService): (Cluster) ⇒ (Row) ⇒ Observable[Row] = {
        client ⇒
          outer ⇒ 
            cassandraR[CassandraObservable#Record](relation(outer), client, c, res, log, ctx)
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