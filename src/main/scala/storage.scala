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
import mongo.channel.DBChannel
import org.apache.log4j.Logger
import com.mongodb.MongoException
import com.datastax.driver.core.Cluster
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Subscriber, Observable }
import scala.annotation.{implicitNotFound, tailrec}
import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure, Try}
import scalaz.concurrent.Task
import scalaz.stream.{ Cause, io }
import scalaz.stream.Process
import scalaz.syntax.id._

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

    def client: Module#Client

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
                             logger: Logger, client: MongoObservable#Client, subscriber: Subscriber[MongoObservable#Record],
                             ctx: MongoObservable#Context) extends ObservableProducer[MongoObservable] {
      override lazy val cursor: Try[MongoObservable#Cursor] = Try {
        val qs = implicitly[QueryInterpreter[MongoObservable]].interpret(settings)
        val cursor = client.getDB(resource).getCollection(collection).find(qs.query)
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
                                 client: CassandraObservable#Client, subscriber: Subscriber[CassandraObservable#Record],
                                 ctx: CassandraObservable#Context) extends ObservableProducer[CassandraObservable] {
      private val defaultPageSize = 8

      override lazy val cursor: Try[CassandraObservable#Cursor] = Try {
        val qs = implicitly[QueryInterpreter[CassandraObservable]].interpret(settings)
        val query = MessageFormat.format(qs.query, collection)
        val session = client.connect(resource)
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
              log: Logger, client: MongoObservable#Client,
              subscriber: Subscriber[MongoObservable#Record],
              ctx: MongoObservable#Context) =
      MongoProducer(settings, collection, resource, log, client, subscriber, ctx)

    def cassandra(settings: QFree[CassandraObservable#QueryAttributes],
                  collection: String, resource: String, logger: Logger,
                  client: CassandraObservable#Client,
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
                        log: Logger, client: MongoObservable#Client,
                        subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoProducer(settings, collection, resource, log, client, subscriber, ctx) with MongoProducerOnFetchError

    def mongoOnCursorLookupError(settings: QFree[MongoObservable#QueryAttributes],
                                 collection: String, resource: String,
                                 log: Logger, client: MongoObservable#Client,
                                 subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoProducer(settings, collection, resource, log, client, subscriber, ctx) with MongoProducerOnCursorError

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
                            client: CassandraObservable#Client,
                            subscriber: Subscriber[CassandraObservable#Record],
                            ctx: CassandraObservable#Context) =
      new CassandraProducer(settings, collection, resource, logger, client, subscriber, ctx) with CassandraProducerOnFetchError

    def cassandraCursorError(settings: QFree[CassandraObservable#QueryAttributes],
                             collection: String, resource: String, logger: Logger,
                             client: CassandraObservable#Client, subscriber: Subscriber[CassandraObservable#Record],
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

  @implicitNotFound(msg = "Cannot find Storage type class for ${T}")
  sealed trait Storage[T <: StorageModule] {
    def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
              log: Logger, ctx: T#Context): T#Client ⇒ T#Stream[T#Record]

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

      override def inner(r: (CassandraAkkaStream#Record) => QFree[CassandraAkkaStream#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: CassandraAkkaStream#Context):
      (CassandraAkkaStream#Client) => (CassandraAkkaStream#Record) => CassandraAkkaStream#Stream[CassandraAkkaStream#Record] = {
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

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      private def mongoObs(qs: QFree[MongoObservable#QueryAttributes], collection: String, resource: String,
                           logger: Logger, client: MongoObservable#Client, ctx: MongoObservable#Context) =
        Observable { subscriber: Subscriber[MongoObservable#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongo(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[MongoObservable#QueryAttributes], collection: String, resource: String,
                         logger: Logger, ctx: MongoObservable#Context): (MongoObservable#Client) ⇒ Observable[MongoObservable#Record] =
        client ⇒ mongoObs(q, collection, resource, logger, client, ctx)

      override def inner(relation: (MongoObservable#Record) ⇒ QFree[MongoReadSettings],
                         collection: String, resource: String, logger: Logger,
                         ctx: MongoObservable#Context): (MongoObservable#Client) ⇒ (MongoObservable#Record) ⇒ Observable[MongoObservable#Record] =
        client ⇒
          outer ⇒ mongoObs(relation(outer), collection, resource, logger, client, ctx)
    }

    implicit object MongoObsCursorError extends Storage[MongoObsCursorError] {
      private type T = MongoObsCursorError

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                                      logger: Logger, client: T#Client, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongoOnCursorLookupError(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => T#Stream[T#Record] =
        client => mongoObsCursorError(q, collection, resource, log, client, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => (T#Record) => Observable[T#Record] =
        client =>
          outer =>
            mongoObsCursorError(relation(outer), collection, resource, log, client, ctx)
    }

    implicit object MongoObsFetchError extends Storage[MongoObsFetchError] {
      private type T = MongoObsFetchError

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                                      logger: Logger, client: T#Client, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.mongoFetchError(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => T#Stream[T#Record] =
        client => mongoObsCursorError(q, collection, resource, log, client, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => (T#Record) => Observable[T#Record] =
        client =>
          outer =>
            mongoObsCursorError(relation(outer), collection, resource, log, client, ctx)

    }



    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      private def cassandraObs(qs: QFree[CassandraReadSettings], client: CassandraObservable#Client,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[CassandraObservable#Record] = {
        Observable { subscriber: Subscriber[CassandraObservable#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandra(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(settings: QFree[CassandraObservable#QueryAttributes],
                         collection: String, resource: String,
                         logger: Logger, ctx: CassandraObservable#Context): (Cluster) ⇒ Observable[CassandraObservable#Record] =
        client ⇒
          cassandraObs(settings, client, collection, resource, logger, ctx)

      override def inner(relation: (CassandraObservable#Record) ⇒ QFree[CassandraObservable#QueryAttributes],
                         collection: String, resource: String,
                         log: Logger, ctx: CassandraObservable#Context): (CassandraObservable#Client) ⇒ (CassandraObservable#Record) ⇒ Observable[CassandraObservable#Record] =
        client ⇒
          outer ⇒
            cassandraObs(relation(outer), client, collection, resource, log, ctx)
    }

    implicit object CassandraObsCursorError extends Storage[CassandraObsCursorError] {
      private type T = CassandraObsCursorError

      private def cassandraObs(qs: QFree[T#QueryAttributes], client: T#Client,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandraCursorError(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => T#Stream[T#Record] =
        client => cassandraObs(qs, client, collection, resource, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => (T#Record) => T#Stream[T#Record] =
        client ⇒
          outer ⇒
            cassandraObs(relation(outer), client, collection, resource, log, ctx)
    }

    implicit object CassandraObsFetchError extends Storage[CassandraObsFetchError] {
      private type T = CassandraObsFetchError

      private def cassandraObs(qs: QFree[T#QueryAttributes], client: T#Client,
                               collection: String, resource: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableProducer.cassandraFetchError(qs, collection, resource, logger, client, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => T#Stream[T#Record] =
        client => cassandraObs(qs, client, collection, resource, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, resource: String,
                         log: Logger, ctx: T#Context): (T#Client) => (T#Record) => T#Stream[T#Record] =
        client ⇒
          outer ⇒
            cassandraObs(relation(outer), client, collection, resource, log, ctx)
    }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      private def mongoR(query: QFree[MongoProcess#QueryAttributes], client: MongoProcess#Client,
                         collection: String, resource: String, logger: Logger): Process[Task, MongoProcess#Record] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[MongoProcess]].interpret(query)
          val cursor = client.getDB(resource).getCollection(collection).find(qs.query)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s"Create Process-Fetcher for query from $collection Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.query} ]")
          cursor
        })(c ⇒ Task.delay {
          logger.info(s"The cursor has been closed"); c.close()
        }) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r
            } else {
              throw Cause.Terminated(Cause.End)
            }
          }
        }

      override def outer(qs: QFree[MongoProcess#QueryAttributes], collection: String, resource: String,
                         logger: Logger, ctx: MongoProcess#Context): (MongoProcess#Client) ⇒ DBChannel[MongoProcess#Client, MongoProcess#Record] =
        client ⇒
          DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
            Task.delay(mongoR(qs, client, collection, resource, logger))
          }(ctx)))

      override def inner(relation: (MongoProcess#Record) ⇒ QFree[MongoProcess#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: MongoProcess#Context): (MongoProcess#Client) ⇒ (MongoProcess#Record) ⇒ DBChannel[MongoProcess#Client, MongoProcess#Record] = {
        client ⇒
          outer ⇒
            DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task { client: MongoProcess#Client ⇒
              Task.delay(mongoR(relation(outer), client, collection, resource, logger))
            }(ctx)))
      }
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      private def cassandraResource(qs: QFree[CassandraProcess#QueryAttributes], client: CassandraProcess#Client,
                                    collection: String, resource: String, logger: Logger): Process[Task, CassandraProcess#Record] =
        Process.await(Task.delay(client.connect(resource))) { session =>
          io.resource(Task.delay {
            val settings = implicitly[QueryInterpreter[CassandraProcess]].interpret(qs)
            val query = MessageFormat.format(settings.query, collection)
            logger.debug(s"★ ★ ★ Create Process-Fetcher for query: Query:[ $query ] Param: [ ${settings.v} ]")
            settings.v.fold(session.execute(session.prepare(query).setConsistencyLevel(settings.consistencyLevel).bind()).iterator) { r ⇒
              session.execute(session.prepare(query).setConsistencyLevel(settings.consistencyLevel).bind(r.v)).iterator
            }
          })(c ⇒ Task.delay {
            logger.info("★ ★ ★ The cursor has been exhausted")
            session.close()
          }) { c ⇒ Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r
            } else {
              throw Cause.Terminated(Cause.End)
            }
            }
          }
        }

      override def outer(qs: QFree[CassandraProcess#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: CassandraProcess#Context): (CassandraProcess#Client) ⇒ DBChannel[CassandraProcess#Client, CassandraProcess#Record] =
        client ⇒
          DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
            Task.delay(cassandraResource(qs, client, collection, resource, logger))
          }(ctx)))

      override def inner(relation: (CassandraProcess#Record) ⇒ QFree[CassandraProcess#QueryAttributes],
                         collection: String, resource: String, logger: Logger,
                         ctx: CassandraProcess#Context): (CassandraProcess#Client) ⇒ (CassandraProcess#Record) ⇒ DBChannel[CassandraProcess#Client, CassandraProcess#Record] =
        client ⇒
          outer ⇒
            DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task { client: CassandraProcess#Client ⇒
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