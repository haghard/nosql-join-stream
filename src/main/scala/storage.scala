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
import join.cassandra._
import join.mongo._
import mongo.channel.ScalazChannel
import org.slf4j.Logger
import com.mongodb.MongoException
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Subscriber, Observable }
import scala.annotation.{implicitNotFound, tailrec}
import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure, Try}
import scalaz.concurrent.Task
import scalaz.stream.{ Cause, io }
import scalaz.stream.Process
import scalaz.syntax.id._
import _root_.mongo.channel.AkkaChannel

package object storage {

  private def scheduler(exec: ExecutorService) =
    ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

  object QueryInterpreter {
    import scalaz.Free.runFC
    import join.mongo.{MongoObservable, MongoProcess, MongoReadSettings}
    import join.cassandra.{CassandraObservable, CassandraProcess, CassandraReadSettings}

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

  private[storage] trait ObservableProducer[Module <: StorageModule] {
    def resource: String

    def collection: String

    def session: Module#Session

    def settings: QFree[Module#QueryAttributes]

    def logger: Logger

    def subscriber: Subscriber[Module#Record]

    def cursor: Try[Module#Cursor]

    def producer(): Long ⇒ Unit

    def fetch(n: Long)
  }

  object ObservableProducer {

    import QueryInterpreter._

    case class MongoProducer(settings: QFree[MongoObservable#QueryAttributes], collection: String, resource: String,
                             logger: Logger, session: MongoObservable#Session, subscriber: Subscriber[MongoObservable#Record],
                             ctx: MongoObservable#Context) extends ObservableProducer[MongoObservable] {
      override lazy val cursor: Try[MongoObservable#Cursor] = Try {
        val qs = implicitly[QueryInterpreter[MongoObservable]].interpret(settings)
        val cursor = session.getCollection(collection).find(qs.query)
        cursor |> { c ⇒
          qs.sort.foreach(c.sort)
          qs.skip.foreach(c.skip)
          qs.limit.foreach(c.limit)
        }
        cursor
      }

      override def fetch(n: Long): Unit =
        cursor match {
          case Success(c) => go(n, c)
          case Failure(ex) ⇒ subscriber.onError(ex)
        }

      override val producer: Long ⇒ Unit =
        n ⇒
          try fetch(n)
          catch {
            case e: Exception =>
              subscriber.onError(e)
              cursor.foreach(_.close())
          }

      @tailrec private final def go(n: Long, cur: MongoObservable#Cursor): Unit =
        if (n > 0) {
          if (cur.hasNext()) {
            subscriber.onNext(cursor.get.next())
            go(n - 1, cur)
          } else {
            subscriber.onCompleted()
            cursor.foreach(_.close())
            logger.info(s"MongoObservableCursor has been exhausted")
          }
        }
    }

    case class CassandraProducer(settings: QFree[CassandraObservable#QueryAttributes],
                                 collection: String, resource: String, logger: Logger,
                                 session: CassandraObservable#Session, subscriber: Subscriber[CassandraObservable#Record],
                                 ctx: CassandraObservable#Context) extends ObservableProducer[CassandraObservable] {
      private val defaultPageSize = 8

      override lazy val cursor: Try[CassandraObservable#Cursor] = Try {
        val qs = implicitly[QueryInterpreter[CassandraObservable]].interpret(settings)
        val query = MessageFormat.format(qs.query, collection)
        //val session = session.connect(resource)
        logger.debug(s"★ ★ ★ Create Observable-Fetcher for query: Query:[ $query ] Param: [ ${qs.v} ]")
        qs.v.fold(session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind()).iterator()) { r ⇒
          session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind(r.v)).iterator()
        }
      }

      @tailrec private def go(n: Int, i: Int, c: CassandraObservable#Cursor): Unit = {
        if (i < n && c.hasNext && !subscriber.isUnsubscribed) {
          subscriber.onNext(c.next())
          go(n, i + 1, c)
        }
      }

      override val producer: (Long) ⇒ Unit =
        n ⇒
          try fetch(n)
          catch {
            case e: Exception => subscriber.onError(e)
          }

      override def fetch(n: Long) = {
        cursor match {
          case Success(c) =>
            val intN = if (n >= defaultPageSize) {
              defaultPageSize
            } else {
              n.toInt
            }
            if (c.hasNext) {
              go(intN, 0, c)
            }
            else {
              subscriber.onCompleted()
            }
          case Failure(ex) ⇒ subscriber.onError(ex)
        }
      }
    }

    def mongo(settings: QFree[MongoObservable#QueryAttributes],
              collection: String, resource: String,
              log: Logger, session: MongoObservable#Session,
              subscriber: Subscriber[MongoObservable#Record],
              ctx: MongoObservable#Context) =
      MongoProducer(settings, collection, resource, log, session, subscriber, ctx)

    def cassandra(settings: QFree[CassandraObservable#QueryAttributes],
                  collection: String, resource: String, logger: Logger,
                  client: CassandraObservable#Session,
                  subscriber: Subscriber[CassandraObservable#Record],
                  ctx: CassandraObservable#Context) =
      CassandraProducer(settings, collection, resource, logger, client, subscriber, ctx)


    trait MongoProducerOnFetchError extends MongoProducer {
      override def fetch(n: Long) =
        throw new MongoException("Mongo error during fetch cursor")
    }

    trait MongoProducerOnCursorError extends MongoProducer {
      override lazy val cursor: Try[MongoObservable#Cursor] = Try {
        throw new MongoException("Creation mongo cursor error")
      }
    }

    def mongoFetchError(settings: QFree[MongoObservable#QueryAttributes],
                        collection: String, resource: String,
                        log: Logger, session: MongoObservable#Session,
                        subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoProducer(settings, collection, resource, log, session, subscriber, ctx) with MongoProducerOnFetchError

    def mongoOnCursorLookupError(settings: QFree[MongoObservable#QueryAttributes],
                                 collection: String, resource: String,
                                 log: Logger, session: MongoObservable#Session,
                                 subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoProducer(settings, collection, resource, log, session, subscriber, ctx) with MongoProducerOnCursorError

    trait CassandraProducerOnFetchError extends CassandraProducer {
      override def fetch(n: Long) =
        throw new Exception("Cassandra error during fetch cursor")
    }

    trait CassandraProducerOnCursorError extends CassandraProducer {
      override lazy val cursor: Try[CassandraObservable#Cursor] = Try {
        throw new Exception("Creation cassandra cursor error")
      }
    }

    def cassandraFetchError(settings: QFree[CassandraObservable#QueryAttributes],
                            collection: String, resource: String, logger: Logger,
                            client: CassandraObservable#Session,
                            subscriber: Subscriber[CassandraObservable#Record],
                            ctx: CassandraObservable#Context) =
      new CassandraProducer(settings, collection, resource, logger, client, subscriber, ctx) with CassandraProducerOnFetchError

    def cassandraCursorError(settings: QFree[CassandraObservable#QueryAttributes],
                             collection: String, resource: String, logger: Logger,
                             client: CassandraObservable#Session, subscriber: Subscriber[CassandraObservable#Record],
                             ctx: CassandraObservable#Context) =
      new CassandraProducer(settings, collection, resource, logger, client, subscriber, ctx) with CassandraProducerOnCursorError
  }

  trait QueryInterpreter[T <: StorageModule] {
    def interpret(q: QFree[T#QueryAttributes]): T#QueryAttributes
  }

  trait DbIterator[Module <: StorageModule] extends Iterator[Module#Record] {
    def logger: Logger
    def resource: String
    def collection: String
    def session: Module#Session

    def settings: QFree[Module#QueryAttributes]

    def cursor: Module#Cursor

    def attributes: Module#QueryAttributes

    override def hasNext = cursor.hasNext

    override def next(): Module#Record = cursor.next()
  }

  object DbIterator {
    case class CassandraIterator(settings: QFree[CassandraSource#QueryAttributes], session: CassandraSource#Session,
                                 resource: String, collection: String, logger: Logger) extends DbIterator[CassandraSource] {
      override val attributes = implicitly[QueryInterpreter[CassandraProcess]].interpret(settings)
      override val cursor = {
        val queryStr = MessageFormat.format(attributes.query, collection)
        attributes.v.fold(session.execute(queryStr).iterator()) { r ⇒
          (session execute(queryStr, r.v)).iterator()
        }
      }
    }

    case class MongoIterator(settings: QFree[MongoSource#QueryAttributes], session: MongoSource#Session,
                             resource: String, collection: String, logger: Logger) extends DbIterator[MongoSource] {
      override val attributes = implicitly[QueryInterpreter[MongoProcess]].interpret(settings)
      override val cursor = {
        val local = session.getCollection(collection).find(attributes.query)
        local |> { c ⇒
          attributes.sort.foreach(c.sort)
          attributes.skip.foreach(c.skip)
          attributes.limit.foreach(c.limit)
        }
        local
      }
    }

    def mongo(settings: QFree[MongoSource#QueryAttributes], session: MongoSource#Session,
              resource: String, collection: String, logger: Logger) =
      MongoIterator(settings, session, resource, collection, logger)

    def cassandra(settings: QFree[CassandraSource#QueryAttributes], session: CassandraSource#Session,
                  resource: String, collection: String, logger: Logger) = {
      CassandraIterator(settings, session, resource, collection, logger)
    }
  }

  @implicitNotFound(msg = "Cannot find Storage type class for ${T}")
  sealed trait Storage[T <: StorageModule] {

    def connect(client: T#Client, resource: String):T#Session

    def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
              log: Logger, ctx: T#Context): T#Session ⇒ T#Stream[T#Record]

    def inner(r: T#Record ⇒ QFree[T#QueryAttributes], collection: String, resource: String,
              log: Logger, ctx: T#Context): T#Session ⇒ (T#Record ⇒ T#Stream[T#Record])
  }

  object Storage {
    import join.mongo.{MongoObservable, MongoProcess, MongoSource, MongoReadSettings}
    import join.cassandra.{CassandraObservable, CassandraProcess, CassandraSource, CassandraReadSettings}

    implicit object CassandraStorageAkkaStream extends Storage[CassandraSource] {
      type T = CassandraSource

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context):
      (T#Session) => T#Stream[T#Record] =
        session =>
          AkkaChannel(Source(() => DbIterator.cassandra(qs, session, resource, collection, log)))

      override def inner(r: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String, log: Logger,
                         ctx: CassandraSource#Context):
        (T#Session) =>
          (T#Record) =>
            T#Stream[T#Record] = {
              session =>
                outer =>
                  AkkaChannel(Source(() => DbIterator.cassandra(r(outer), session, resource, collection, log)))
      }
    }

    implicit object MongoStorageAkkaStream extends Storage[MongoSource] {
      type T = MongoSource

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      override def outer(qs: QFree[T#QueryAttributes], collection: String,
                         resource: String, log: Logger, ctx: MongoSource#Context):
                         (T#Session) => T#Stream[T#Record] =
        session =>
          AkkaChannel(Source(() => DbIterator.mongo(qs, session, resource, collection, log)))

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String,
                         resource: String, log: Logger, ctx: T#Context):
        (T#Session) => (T#Record) => T#Stream[T#Record] =
          session =>
             outer =>
               AkkaChannel(Source(() => DbIterator.mongo(relation(outer), session, resource, collection, log)))

    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      type T = MongoObservable

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObs(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                           logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongo(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
                         logger: Logger, ctx: T#Context): (T#Session) ⇒ Observable[T#Record] =
        session ⇒ mongoObs(q, collection, resource, logger, session, ctx)

      override def inner(relation: (T#Record) ⇒ QFree[MongoReadSettings],
                         collection: String, resource: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ Observable[T#Record] =
        session ⇒
          outer ⇒ mongoObs(relation(outer), collection, resource, logger, session, ctx)
    }

    implicit object MongoObsCursorError extends Storage[MongoObsCursorError] {
      private type T = MongoObsCursorError

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                                      logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongoOnCursorLookupError(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session => mongoObsCursorError(q, collection, resource, log, session, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => Observable[T#Record] =
        session =>
          outer =>
            mongoObsCursorError(relation(outer), collection, resource, log, session, ctx)
    }

    implicit object MongoObsFetchError extends Storage[MongoObsFetchError] {
      private type T = MongoObsFetchError

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                                      logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongoFetchError(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session =>
          mongoObsCursorError(q, collection, resource, log, session, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => Observable[T#Record] =
        session =>
          outer =>
            mongoObsCursorError(relation(outer), collection, resource, log, session, ctx)

    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      type T = CassandraObservable

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObs(qs: QFree[CassandraReadSettings], session: T#Session,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandra(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(settings: QFree[T#QueryAttributes],
                         collection: String, resource: String,
                         logger: Logger, ctx: T#Context): (T#Session) ⇒ T#Stream[T#Record] =
        session ⇒
          cassandraObs(settings, session, collection, resource, logger, ctx)

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes],
                         collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ Observable[T#Record] =
        session ⇒
          outer ⇒
            cassandraObs(relation(outer), session, collection, resource, log, ctx)
    }

    implicit object CassandraObsCursorError extends Storage[CassandraObsCursorError] {
      private type T = CassandraObsCursorError

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObs(qs: QFree[T#QueryAttributes], session: T#Session,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandraCursorError(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session =>
          cassandraObs(qs, session, collection, resource, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => T#Stream[T#Record] =
        session ⇒
          outer ⇒
            cassandraObs(relation(outer), session, collection, resource, log, ctx)
    }

    implicit object CassandraObsFetchError extends Storage[CassandraObsFetchError] {
      private type T = CassandraObsFetchError

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObs(qs: QFree[T#QueryAttributes], session: T#Session,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandraFetchError(qs, collection, resource, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session => cassandraObs(qs, session, collection, resource, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => T#Stream[T#Record] =
        session ⇒
          outer ⇒
            cassandraObs(relation(outer), session, collection, resource, log, ctx)
    }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      type T = MongoProcess

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoR(query: QFree[T#QueryAttributes], client: T#Session,
                         collection: String, resource: String, logger: Logger): Process[Task, T#Record] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[T]].interpret(query)
          val cursor = client.getCollection(collection).find(qs.query)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s"Create Process-Fetcher for query from $collection Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.query} ]")
          cursor
        })(c ⇒ Task.delay {
          logger.info(s"The cursor has been closed");
          c.close()
        }) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r
            } else throw Cause.Terminated(Cause.End)
          }
        }

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String, logger: Logger, ctx: T#Context):
        (T#Session) ⇒ ScalazChannel[T#Session, T#Record] =
          session ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { session: T#Session ⇒
              Task.delay(mongoR(qs, session, collection, resource, logger))
            }(ctx)))

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ ScalazChannel[T#Session, T#Record] = {
        session ⇒
          outer ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
              Task.delay(mongoR(relation(outer), client, collection, resource, logger))
            }(ctx)))
      }
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      type T = CassandraProcess

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraResource(qs: QFree[T#QueryAttributes], session: T#Session,
                                    collection: String, resource: String, logger: Logger): Process[Task, T#Record] =
        Process.await(Task.delay(session)) { session =>
          io.resource(Task.delay {
            val attributes = implicitly[QueryInterpreter[T]].interpret(qs)
            val query = MessageFormat.format(attributes.query, collection)
            logger.debug(s"★ ★ ★ Create Process-Fetcher for query: query:[ $query ] Param: [ ${attributes.v} ]")
            attributes.v.fold(session.execute(query).iterator()) { r ⇒
              (session execute(query, r.v)).iterator()
            }
          })(c ⇒ Task.delay {
            logger.debug("★ ★ ★ The cursor has been exhausted ★ ★ ★")
          }) { c ⇒ Task.delay {
              if (c.hasNext) {
                val r = c.next
                logger.debug(s"fetch $r")
                r
              } else throw Cause.Terminated(Cause.End)
            }
          }
        }

      override def outer(qs: QFree[T#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ ScalazChannel[T#Session, T#Record] =
        session ⇒
          ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
            Task.delay(cassandraResource(qs, client, collection, resource, logger))
          }(ctx)))

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ ScalazChannel[T#Session, T#Record] =
        session ⇒
          outer ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
              Task.delay(cassandraResource(relation(outer), client, collection, resource, logger))
            }(ctx)))
    }

    /**
     *
     * @tparam T
     * @return
     */
    def apply[T <: StorageModule: Storage]: Storage[T] = implicitly[Storage[T]]
  }
}