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

import java.io.Closeable
import java.text.MessageFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.{AtomicReference, AtomicLong}
import akka.stream.scaladsl.Source
import com.datastax.driver.core.{Row, Session}
import dsl.QFree
import dsl.cassandra.CassandraQueryInterpreter
import dsl.mongo.MongoQueryInterpreter
import join.StorageModule
import join.cassandra._
import join.mongo._
import mongo.channel.ScalazChannel
import org.slf4j.Logger
import com.mongodb.{DBObject, DB, MongoException}
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

import java.lang.{Long => JLong}

//Split them up
package object storage {

  private def scheduler(exec: ExecutorService) =
    ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

  private def navigatePartition(sequenceNr: Long, maxPartitionSize: Long) = sequenceNr / maxPartitionSize

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

  trait ObservableDBProducer[Module <: StorageModule] extends Closeable {
    def session: Module#Session
    def fetch(n: Long)
    def cursor: Try[Module#Cursor]
    def logger: Logger
    def subscriber: Subscriber[Module#Record]
    def producer: Long ⇒ Unit =
      n ⇒
        try fetch(n)
        catch {
          case e: Exception =>
            subscriber.onError(e)
            close()
        }
  }

  trait ObservableJoinProducer[Module <: StorageModule] extends ObservableDBProducer[Module] {
    def collection: String
    def settings: QFree[Module#QueryAttributes]
  }

  trait ObservableStreamProducer[Module <: StorageModule] extends ObservableDBProducer[Module] {
    def query: String
    def key: String
    def offset: Long
    def maxPartitionSize: Long
  }

  object ObservableJoinProducer {
    import QueryInterpreter._

    case class MongoJoinProducer(settings: QFree[MongoObservable#QueryAttributes], collection: String,
                                 logger: Logger, session: MongoObservable#Session, subscriber: Subscriber[MongoObservable#Record],
                                 ctx: MongoObservable#Context) extends ObservableJoinProducer[MongoObservable] {
      override lazy val cursor: Try[MongoObservable#Cursor] = Try {
        val qs = (implicitly[QueryInterpreter[MongoObservable]] interpret settings)
        logger.debug(s"★ ★ ★ mongo-join-observable for query: $qs")
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
          case Success(c) ⇒ go(n, c)
          case Failure(ex) ⇒ (subscriber onError ex)
        }

      @tailrec final def go(n: Long, cur: MongoObservable#Cursor): Unit =
        if (n > 0) {
          if (cur.hasNext) {
            (subscriber onNext cursor.get.next)
            go(n - 1, cur)
          } else {
            subscriber.onCompleted()
            cursor.foreach(_.close())
            logger.debug(s"MongoObservableCursor has been exhausted")
          }
        }

      override def close: Unit =
        cursor.foreach(_.close())
    }

    case class CassandraStreamProducer(session: CassandraObservable#Session, query: String, key: String, offset: Long,
                                       maxPartitionSize: Long, logger: Logger, subscriber: Subscriber[CassandraObservable#Record],
                                       defaultPageSize: Int = 8) extends ObservableStreamProducer[CassandraObservable] {
      val seqNum = new AtomicLong(offset)
      val cursorRef = new AtomicReference[Try[CassandraObservable#Cursor]](cursor)

      override def cursor: Try[CassandraObservable#Cursor] = {
        Try {
          val cassandraQuery = MessageFormat.format(query, key)
          val partition = navigatePartition(seqNum.get(), maxPartitionSize)
          logger.debug(s"★ ★ ★ cassandra-stream-observable for query: $query Key:$key Partition: $partition")
          (session.execute(cassandraQuery, key: String, partition: JLong, offset: JLong)).iterator()
        }
      }

      @tailrec final def loop(n: Int, i: Int, c: CassandraObservable#Cursor): Unit = {
        if (i < n && c.hasNext && !subscriber.isUnsubscribed) {
          (subscriber onNext c.next)
          if(seqNum.incrementAndGet() % maxPartitionSize == 0) {
            cursorRef.set(cursor)//switch on next partition
          }
          loop(n, i + 1, c)
        }
      }

      override def fetch(n: Long) = {
        cursorRef.get() match {
          case Success(c) =>
            val request = if (n >= defaultPageSize) defaultPageSize else n.toInt
            if (c.hasNext) loop(request, 0, c)
            else subscriber.onCompleted()
          case Failure(ex) ⇒ (subscriber onError ex)
        }
      }

      override def close: Unit = ()
    }

    case class CassandraJoinProducer(settings: QFree[CassandraObservable#QueryAttributes], collection: String, logger: Logger,
                                     session: CassandraObservable#Session, subscriber: Subscriber[CassandraObservable#Record],
                                     ctx: CassandraObservable#Context, defaultPageSize: Int = 8) extends ObservableJoinProducer[CassandraObservable] {
      override lazy val cursor: Try[CassandraObservable#Cursor] = Try {
        val attributes = (implicitly[QueryInterpreter[CassandraObservable]] interpret settings)
        val query = MessageFormat.format(attributes.query, collection)
        logger.debug(s"★ ★ ★ cassandra-join-observable for query:$query Param:[ ${attributes.v} ]")
        attributes.v.fold(session.execute(query).iterator()) { r ⇒
          (session execute(query, r.v)).iterator()
        }
      }

      @tailrec final def go(n: Int, i: Int, c: CassandraObservable#Cursor): Unit = {
        if (i < n && c.hasNext && !subscriber.isUnsubscribed) {
          (subscriber onNext c.next)
          go(n, i + 1, c)
        }
      }

      override def fetch(n: Long) = {
        cursor match {
          case Success(c) =>
            val intN = if (n >= defaultPageSize) defaultPageSize else n.toInt
            if (c.hasNext) go(intN, 0, c)
            else subscriber.onCompleted()
          case Failure(ex) ⇒ (subscriber onError ex)
        }
      }

      override def close: Unit = ()
    }

    def mongoJoin(settings: QFree[MongoObservable#QueryAttributes],
                  collection: String, log: Logger,
                  session: MongoObservable#Session,
                  subscriber: Subscriber[MongoObservable#Record],
                  ctx: MongoObservable#Context) =
      MongoJoinProducer(settings, collection, log, session, subscriber, ctx)

    def cassandraJoin(settings: QFree[CassandraObservable#QueryAttributes],
                      collection: String, logger: Logger,
                      session: CassandraObservable#Session,
                      subscriber: Subscriber[CassandraObservable#Record],
                      ctx: CassandraObservable#Context) =
      CassandraJoinProducer(settings, collection, logger, session, subscriber, ctx)

    def cassandraStream(session: CassandraObservable#Session, query: String, key: String,
                        offset: Long, maxPartitionSize: Long, log: Logger,
                        subscriber: Subscriber[CassandraObservable#Record]) =
      CassandraStreamProducer(session, query, key, offset, maxPartitionSize, log, subscriber)

    trait MongoProducerOnFetchError extends MongoJoinProducer {
      override def fetch(n: Long) =
        throw new MongoException("Mongo error during fetch cursor")
    }

    trait MongoProducerOnCursorError extends MongoJoinProducer {
      override lazy val cursor: Try[MongoObservable#Cursor] = Try {
        throw new MongoException("Creation mongo cursor error")
      }
    }

    def mongoFetchError(settings: QFree[MongoObservable#QueryAttributes],
                        collection: String, log: Logger, session: MongoObservable#Session,
                        subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoJoinProducer(settings, collection, log, session, subscriber, ctx) with MongoProducerOnFetchError

    def mongoOnCursorLookupError(settings: QFree[MongoObservable#QueryAttributes],
                                 collection: String, log: Logger, session: MongoObservable#Session,
                                 subscriber: Subscriber[MongoObservable#Record], ctx: MongoObservable#Context) =
      new MongoJoinProducer(settings, collection, log, session, subscriber, ctx) with MongoProducerOnCursorError

    trait CassandraProducerOnFetchError extends CassandraJoinProducer {
      override def fetch(n: Long) =
        throw new Exception("Cassandra error during fetch cursor")
    }

    trait CassandraProducerOnCursorError extends CassandraJoinProducer {
      override lazy val cursor: Try[CassandraObservable#Cursor] = Try {
        throw new Exception("Creation cassandra cursor error")
      }
    }

    def cassandraFetchError(settings: QFree[CassandraObservable#QueryAttributes], collection: String,
                            logger: Logger, client: CassandraObservable#Session, subscriber: Subscriber[CassandraObservable#Record],
                            ctx: CassandraObservable#Context) =
      new CassandraJoinProducer(settings, collection, logger, client, subscriber, ctx) with CassandraProducerOnFetchError

    def cassandraCursorError(settings: QFree[CassandraObservable#QueryAttributes], collection: String, logger: Logger,
                            client: CassandraObservable#Session, subscriber: Subscriber[CassandraObservable#Record],
                            ctx: CassandraObservable#Context) =
      new CassandraJoinProducer(settings, collection, logger, client, subscriber, ctx) with CassandraProducerOnCursorError
  }

  trait QueryInterpreter[T <: StorageModule] {
    def interpret(q: QFree[T#QueryAttributes]): T#QueryAttributes
  }

  trait DbIterator[Module <: StorageModule] extends Iterator[Module#Record] {
    def logger: Logger
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
                                 collection: String, logger: Logger) extends DbIterator[CassandraSource] {
      override val attributes = (implicitly[QueryInterpreter[CassandraProcess]] interpret settings)
      override val cursor = {
        val query = MessageFormat.format(attributes.query, collection)
        attributes.v.fold(session.execute(query).iterator()) { r ⇒
          (session execute(query, r.v)).iterator()
        }
      }
    }

    case class MongoIterator(settings: QFree[MongoSource#QueryAttributes], session: MongoSource#Session,
                             collection: String, logger: Logger) extends DbIterator[MongoSource] {
      override val attributes = (implicitly[QueryInterpreter[MongoProcess]] interpret settings)
      override val cursor = {
        val dBCursor = (session.getCollection(collection) find attributes.query)
        dBCursor |> { c ⇒
          attributes.sort.foreach(c.sort)
          attributes.skip.foreach(c.skip)
          attributes.limit.foreach(c.limit)
        }
        dBCursor
      }
    }

    case class CassandraStreamIterator(session: CassandraSource#Session, query: String, key: String, offset: Long,
                                       maxPartitionSize: Long, log: Logger) extends Iterator[CassandraSource#Record]() {
      val seqNum = new AtomicLong(offset)
      val cursorRef = new AtomicReference[CassandraSource#Cursor](newIter)

      private def newIter = {
        val p = navigatePartition(seqNum.get(), maxPartitionSize)
        (session execute(query, key: String, p:JLong,  seqNum.get: JLong)).iterator()
      }

      override def hasNext = cursorRef.get().hasNext

      override def next() = {
        val row = cursorRef.get().next()
        if(seqNum.incrementAndGet() % maxPartitionSize == 0) {
          (cursorRef set newIter)
        }
        row
      }
    }

    def mongoJoin(settings: QFree[MongoSource#QueryAttributes], session: MongoSource#Session, collection: String, logger: Logger) =
      MongoIterator(settings, session, collection, logger)

    def cassandraJoin(settings: QFree[CassandraSource#QueryAttributes], session: CassandraSource#Session, collection: String, logger: Logger) =
      CassandraIterator(settings, session, collection, logger)

    def cassandraStream(session: CassandraSource#Session, query: String, key: String, offset: Long, maxPartitionSize: Long, log: Logger) =
      CassandraStreamIterator(session, query, key, offset, maxPartitionSize, log)
  }

  @implicitNotFound(msg = "Cannot find Storage type class for ${T}")
  sealed trait Storage[T <: StorageModule] {

    def connect(client: T#Client, resource: String):T#Session

    def log(session: T#Session, query: String, key: String, offset: Long, maxPartitionSize: Long,
            log: Logger, ctx: T#Context): T#Stream[T#Record]

    def outer(q: QFree[T#QueryAttributes], collection: String,
              log: Logger, ctx: T#Context): T#Session ⇒ T#Stream[T#Record]

    def inner(r: T#Record ⇒ QFree[T#QueryAttributes], collection: String,
              log: Logger, ctx: T#Context): T#Session ⇒ (T#Record ⇒ T#Stream[T#Record])
  }

  object Storage {
    import join.mongo.{MongoObservable, MongoProcess, MongoSource, MongoReadSettings}
    import join.cassandra.{CassandraObservable, CassandraProcess, CassandraSource, CassandraReadSettings}

    implicit object CassandraStorageAkkaStream extends Storage[CassandraSource] {
      type T = CassandraSource

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      override def outer(qs: QFree[T#QueryAttributes], collection: String, log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session =>
          AkkaChannel(Source(() => DbIterator.cassandraJoin(qs, session, collection, log)))

      override def inner(r: (T#Record) => QFree[T#QueryAttributes], collection: String, log: Logger, ctx: CassandraSource#Context):
        (T#Session) =>
          (T#Record) =>
            T#Stream[T#Record] = {
              session =>
                outer =>
                  AkkaChannel(Source(() => DbIterator.cassandraJoin(r(outer), session, collection, log)))
      }

      override def log(session: T#Session, query: String, key: String, offset: Long, maxPartitionSize: Long, log: Logger,
                       ctx: T#Context): T#Stream[T#Record] =
        AkkaChannel(Source(() => DbIterator.cassandraStream(session,query,key, offset, maxPartitionSize, log)))
    }

    implicit object MongoStorageAkkaStream extends Storage[MongoSource] {
      type T = MongoSource

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      override def outer(qs: QFree[T#QueryAttributes], collection: String, log: Logger, ctx: MongoSource#Context):
                         (T#Session) => T#Stream[T#Record] =
        session =>
          AkkaChannel(Source(() => DbIterator.mongoJoin(qs, session, collection, log)))

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String, log: Logger, ctx: T#Context):
        (T#Session) => (T#Record) => T#Stream[T#Record] =
          session =>
             outer =>
               AkkaChannel(Source(() => DbIterator.mongoJoin(relation(outer), session, collection, log)))

      override def log(session: DB, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutionContext): AkkaChannel[DBObject, Unit] = ???
    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      type T = MongoObservable

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObs(qs: QFree[T#QueryAttributes], collection: String,
                           logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.mongoJoin(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String, logger: Logger, ctx: T#Context): (T#Session) ⇒ Observable[T#Record] =
        session ⇒ mongoObs(q, collection, logger, session, ctx)

      override def inner(relation: (T#Record) ⇒ QFree[MongoReadSettings],
                         collection: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ Observable[T#Record] =
        session ⇒
          outer ⇒ mongoObs(relation(outer), collection, logger, session, ctx)

      override def log(session: DB, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): Observable[DBObject] = ???
    }

    implicit object MongoObsCursorError extends Storage[MongoObsCursorError] {
      private type T = MongoObsCursorError

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String,
                                      logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.mongoOnCursorLookupError(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session => mongoObsCursorError(q, collection, log, session, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => Observable[T#Record] =
        session =>
          outer =>
            mongoObsCursorError(relation(outer), collection, log, session, ctx)

      override def log(session: DB, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): Observable[DBObject] = ???
    }

    implicit object MongoObsFetchError extends Storage[MongoObsFetchError] {
      private type T = MongoObsFetchError

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoObsCursorError(qs: QFree[T#QueryAttributes], collection: String,
                                      logger: Logger, session: T#Session, ctx: T#Context) =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.mongoFetchError(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))

      override def outer(q: QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session =>
          mongoObsCursorError(q, collection, log, session, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => Observable[T#Record] =
        session =>
          outer =>
            mongoObsCursorError(relation(outer), collection, log, session, ctx)

      override def log(session: DB, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): Observable[DBObject] = ???
    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      type T = CassandraObservable

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObservable(qs: QFree[CassandraReadSettings], session: T#Session,
                                      collection: String,
                                      logger: Logger, ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.cassandraJoin(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(settings: QFree[T#QueryAttributes], collection: String,
                         logger: Logger, ctx: T#Context): (T#Session) ⇒ T#Stream[T#Record] =
        session ⇒
          cassandraObservable(settings, session, collection, logger, ctx)

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ Observable[T#Record] =
        session ⇒
          outer ⇒
            cassandraObservable(relation(outer), session, collection, log, ctx)

      override def log(session: T#Session, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: T#Context): T#Stream[T#Record] =
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.cassandraStream(session, query, key, offset, maxPartitionSize, log, subscriber).producer)
        }.subscribeOn(scheduler(ctx))
    }

    implicit object CassandraObsCursorError extends Storage[CassandraObsCursorError] {
      private type T = CassandraObsCursorError

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObs(qs: QFree[T#QueryAttributes], session: T#Session,
                               collection: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.cassandraCursorError(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session =>
          cassandraObs(qs, session, collection, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => T#Stream[T#Record] =
        session ⇒
          outer ⇒
            cassandraObs(relation(outer), session, collection, log, ctx)

      override def log(session: Session, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): Observable[Row] = ???
    }

    implicit object CassandraObsFetchError extends Storage[CassandraObsFetchError] {
      private type T = CassandraObsFetchError

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraObs(qs: QFree[T#QueryAttributes], session: T#Session,
                               collection: String, logger: Logger,
                               ctx: ExecutorService): Observable[T#Record] = {
        Observable { subscriber: Subscriber[T#Record] ⇒
          subscriber.setProducer(ObservableJoinProducer.cassandraFetchError(qs, collection, logger, session, subscriber, ctx).producer)
        }.subscribeOn(scheduler(ctx))
      }

      override def outer(qs: QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => T#Stream[T#Record] =
        session => cassandraObs(qs, session, collection, log, ctx)

      override def inner(relation: (T#Record) => QFree[T#QueryAttributes], collection: String,
                         log: Logger, ctx: T#Context): (T#Session) => (T#Record) => T#Stream[T#Record] =
        session ⇒
          outer ⇒
            cassandraObs(relation(outer), session, collection, log, ctx)

      override def log(session: Session, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): Observable[Row] = ???
    }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {
      type T = MongoProcess

      override def connect(client: T#Client, resource: String): T#Session =
        client getDB resource

      private def mongoR(query: QFree[T#QueryAttributes], client: T#Session,
                         collection: String, logger: Logger): Process[Task, T#Record] =
        io.resource(Task.delay {
          val qs = implicitly[QueryInterpreter[T]].interpret(query)
          val cursor = client.getCollection(collection).find(qs.query)
          cursor |> { c ⇒
            qs.sort.foreach(c.sort)
            qs.skip.foreach(c.skip)
            qs.limit.foreach(c.limit)
          }
          logger.debug(s" mongo-join-process from $collection Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.query} ]")
          cursor
        })(c ⇒ Task.delay {
          logger.info(s"The cursor has been closed")
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

      override def outer(qs: QFree[T#QueryAttributes], collection: String, logger: Logger, ctx: T#Context):
        (T#Session) ⇒ ScalazChannel[T#Session, T#Record] =
          session ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { session: T#Session ⇒
              Task.delay(mongoR(qs, session, collection, logger))
            }(ctx)))

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes],
                         collection: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ ScalazChannel[T#Session, T#Record] = {
        session ⇒
          outer ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
              Task.delay(mongoR(relation(outer), client, collection, logger))
            }(ctx)))
      }

      override def log(session: DB, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: ExecutorService): ScalazChannel[DB, DBObject] = ???
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      type T = CassandraProcess

      override def connect(client: T#Client, resource: String):T#Session =
        client connect resource

      private def cassandraResource(qs: QFree[T#QueryAttributes], session: T#Session,
                                    collection: String, logger: Logger): Process[Task, T#Record] =
        io.resource(Task.delay {
          val attributes = (implicitly[QueryInterpreter[T]] interpret qs)
          val query = MessageFormat.format(attributes.query, collection)
          logger.debug(s"★ ★ ★ cassandra-join-process query:[ $query ] param: [ ${attributes.v} ]")
          attributes.v.fold(session.execute(query).iterator()) { r ⇒
            (session execute(query, r.v)).iterator()
          }
        })(c ⇒ Task.delay(logger.debug("★ ★ ★ The cursor has been exhausted ★ ★ ★"))) { c ⇒
          Task.delay {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r
            } else throw Cause.Terminated(Cause.End)
          }
        }

      override def log(session: T#Session, query: String, key: String, offset: Long, maxPartitionSize: Long,
                       log: Logger, ctx: T#Context): ScalazChannel[T#Session, T#Record] = {
        ScalazChannel[T#Session, T#Record](Process.eval(Task { session: T#Session ⇒
          Task.delay {
            def newIter(seqNum: Long): T#Cursor = {
              val cassandraQuery = MessageFormat.format(query, key)
              val p = navigatePartition(seqNum, maxPartitionSize)
              //log.debug(s"★ ★ ★ cassandra-log-process query $query Key:$key Partition: $p - seqNum: $seqNum")
              (session.execute(cassandraQuery, key: String, p: JLong, seqNum: JLong)).iterator()
            }

            def loop(seqNum: Long, iter: T#Cursor): Process[Task, T#Record] = {
              def innerLoop(seqNum: Long, iter: T#Cursor): Process[Task, T#Record] =
                if(iter.hasNext) Process.emit(iter.next()) ++ innerLoop(seqNum + 1, iter)
                else loop(seqNum, newIter(seqNum))

              if (iter.hasNext) innerLoop(seqNum, iter) else Process.halt
            }
            loop(offset, newIter(offset))
          }
        }(ctx)))
      }

      override def outer(qs: QFree[T#QueryAttributes],
                         collection: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ ScalazChannel[T#Session, T#Record] =
        session ⇒
          ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
            Task.delay(cassandraResource(qs, client, collection, logger))
          }(ctx)))

      override def inner(relation: (T#Record) ⇒ QFree[T#QueryAttributes],
                         collection: String, logger: Logger,
                         ctx: T#Context): (T#Session) ⇒ (T#Record) ⇒ ScalazChannel[T#Session, T#Record] =
        session ⇒
          outer ⇒
            ScalazChannel[T#Session, T#Record](Process.eval(Task { client: T#Session ⇒
              Task.delay(cassandraResource(relation(outer), client, collection, logger))
            }(ctx)))
    }

    def apply[T <: StorageModule: Storage]: Storage[T] = implicitly[Storage[T]]
  }
}
