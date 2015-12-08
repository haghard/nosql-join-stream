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

import scala.annotation.implicitNotFound
/**
 *
 * Based on idea from: http://io.pellucid.com/blog/abstract-algebraic-data-type
 */
package object join {
  import dsl.QFree
  import storage.Storage
  import scala.reflect.ClassTag

  trait StorageModule {
    type Record
    type QueryAttributes
    type Cursor <: {
      def hasNext(): Boolean
      def next(): Record
    }
    type Client
    type Session

    type Stream[A] <: {
      def map[B](f: A ⇒ B): Stream[B]
      def flatMap[B](f: A ⇒ Stream[B]): Stream[B]
    }

    type Context
  }

  /**
   * Join is parametrized by DBModule which extends ``[[StorageModule]]`` and requires implicit values
   * [[Joiner[DBModule]]] and [[Storage[DBModule]]] in scope.
   * This is done for compiler to find all implicit instance. Let's call them ``internal dependencies`` for our domain.
   * They define internal structure of the library
   *
   * Also we ask for [[M#Context]] and [[M#Client]]. They are ours external dependencies
   *
   */
  case class Join[M <: StorageModule : Joiner : Storage](implicit ctx: M#Context, client: M#Client, t: ClassTag[M]) {
    implicit val logger = org.slf4j.LoggerFactory.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-producer-join")

    def inner[A](outerQ: QFree[M#QueryAttributes], outerColl: String,
                 innerQ: M#Record ⇒ QFree[M#QueryAttributes], innerColl: String, resource: String)
                (mapper: (M#Record, M#Record) ⇒ A): M#Stream[A] = {
      val storage = Storage[M]
      val session = (storage connect(client, resource))
      val outer = storage.outer(outerQ, outerColl, logger, ctx)(session)
      val relation = storage.inner(innerQ, innerColl, logger, ctx)(session)
      Joiner[M].join[M#Record, M#Record, A](outer)(relation)(mapper)
    }
  }

  @implicitNotFound(msg = "Cannot find Joiner type class for ${T}")
  trait Joiner[T <: StorageModule] {
    def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])(mapper: (A, B) ⇒ C)
                     (implicit ctx: T#Context): T#Stream[C]
  }

  object Joiner {
    import join.mongo.{MongoObservable, MongoProcess, MongoSource, MongoObsCursorError, MongoObsFetchError}
    import join.cassandra.{CassandraObservable, CassandraProcess, CassandraSource, CassandraObsFetchError, CassandraObsCursorError}

    implicit object MongoP extends Joiner[MongoProcess] {
      type T = MongoProcess
      def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])
                       (mapper: (A, B) ⇒ C)
                       (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _)) } yield rs
    }

    implicit object MongoO extends Joiner[MongoObservable] {
      type T = MongoObservable
      override def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])
                                (mapper: (A, B) ⇒ C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _))  } yield rs
    }

    implicit object MongoOCursorError extends Joiner[MongoObsCursorError] {
      type T =  MongoObsCursorError
      override def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])
                                (mapper: (A, B) ⇒ C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for {id ← outer; rs ← relation(id).map(mapper(id, _))} yield rs
    }

    implicit object MongoOFetchError extends Joiner[MongoObsFetchError] {
      type T = MongoObsFetchError
      override def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])
                                (mapper: (A, B) ⇒ C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for {id ← outer; rs ← relation(id).map(mapper(id, _))} yield rs
    }

    implicit object CassandraP extends Joiner[CassandraProcess] {
      type T = CassandraProcess
      override def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])
                                (mapper: (A, B) ⇒ C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _)) } yield rs
    }

    implicit object CassandraO extends Joiner[CassandraObservable] {
      type T = CassandraObservable
      override def join[A, B, C](outer: T#Stream[A])(relation: (A) ⇒ T#Stream[B])
                                (mapper: (A, B) ⇒ C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _))  } yield rs
    }

    implicit object CassandraOCursorError extends Joiner[CassandraObsCursorError] {
      type T = CassandraObsCursorError
      override def join[A, B, C](outer: T#Stream[A])(relation: (A) => T#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for {id ← outer; rs ← relation(id).map(mapper(id, _))} yield rs
    }

    implicit object CassandraOFetchError extends Joiner[CassandraObsFetchError] {
      type T = CassandraObsFetchError
      override def join[A, B, C](outer: T#Stream[A])(relation: (A) => T#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for {id ← outer; rs ← relation(id).map(mapper(id, _))} yield rs
    }


    implicit object MongoASP extends Joiner[MongoSource] {
      type T = MongoSource
      override def join[A, B, C](outer: T#Stream[A])(relation: (A) => T#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _)) } yield rs
    }

    implicit object CassandraASP extends Joiner[CassandraSource] {
      type T = CassandraSource
      override def join[A, B, C](outer: T#Stream[A])(relation: (A) => T#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: T#Context): T#Stream[C] =
        for { id ← outer; rs ← relation(id).map(mapper(id, _)) } yield rs
    }

    /*
    private type AkkaSource[x] = akka.stream.scaladsl.Source[x, Unit]
    private def akkaSequentualSource[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B], cmb: (A, B) => C,
                                              system: akka.actor.ActorSystem): AkkaSource[C] =
      outer.map(a => relation(a).map(b => cmb(a, b)))
        .flatten(akka.stream.scaladsl.FlattenStrategy.concat[C])

    implicit object MongoASP extends Joiner[MongoAkkaStream] {
      override def join[A, B, C](outer: MongoAkkaStream#Stream[A])(relation: (A) => MongoAkkaStream#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: MongoAkkaStream#Context): MongoAkkaStream#Stream[C] =
      ctx.fold(system => akkaSequentualSource(outer, relation, mapper, system), { ctxData =>
        akkaParallelSource(outer, relation, mapper, ctxData.parallelism) (akka.stream.ActorMaterializer(ctxData.setting)(ctxData.system),
          ctxData.S.asInstanceOf[scalaz.Semigroup[C]])
      })
    }

    implicit object CassandraASP extends Joiner[CassandraAkkaStream] {
      override def join[A, B, C](outer: CassandraAkkaStream#Stream[A])(relation: (A) => CassandraAkkaStream#Stream[B])
                                (mapper: (A, B) => C)
                                (implicit ctx: CassandraAkkaStream#Context): CassandraAkkaStream#Stream[C] = {
        ctx.fold(system => akkaSequentualSource(outer, relation, mapper, system), { ctxData =>
          akkaParallelSource(outer, relation, mapper, ctxData.parallelism) (akka.stream.ActorMaterializer(ctxData.setting)(ctxData.system),
            ctxData.S.asInstanceOf[scalaz.Semigroup[C]])
        })
      }
    }

    /**
     * Nondeterministically sequence `fs`, collecting the results with `Semigroup`
     */
    private def akkaParallelSource[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B],
                                            cmb: (A, B) => C, parallelism: Int)
                                           (implicit Mat: akka.stream.ActorMaterializer, S: scalaz.Semigroup[C]): AkkaSource[C] = {
      outer.via(Flow[A].mapAsyncUnordered(parallelism) { ids =>
        relation(ids).map(b => cmb(ids, b)).runFold(List[C]())(_ :+ _)
      }).conflate(_.reduce(S.append(_, _)))((line, list) => S.append(line, list.reduce(S.append(_, _))))
    }

    private def akkaParallelSource2[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B],
                                             cmb: (A, B) => C, parallelism: Int)
                                            (implicit Mat: akka.stream.ActorMaterializer, S: scalaz.Semigroup[C]): AkkaSource[C] =
      outer.grouped(parallelism).map { ids =>
        Source() { implicit b =>
          import FlowGraph.Implicits._
          val innerSource = ids.map(in => relation(in).map(outer => cmb(in, outer)))
          val merge = b.add(Merge[C](ids.size))
          innerSource.foreach(_ ~> merge)
          merge.out
        }
      }.flatten(akka.stream.scaladsl.FlattenStrategy.concat[C])

    */

    /*case class AkkaConcurrentAttributes(setting: akka.stream.ActorMaterializerSettings,
                                        system: akka.actor.ActorSystem, parallelism: Int,
                                        S: scalaz.Semigroup[T] forSome {type T})*/

    def apply[T <: StorageModule: Joiner: Storage]: Joiner[T] = implicitly[Joiner[T]]
  }
}
