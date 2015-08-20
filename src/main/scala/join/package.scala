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

import akka.stream.scaladsl._
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

/**
 *
 * Based on idea from: http://io.pellucid.com/blog/abstract-algebraic-data-type
 *
 * Since akka source isn't the monad we can't use this constraint anymore
 * type Stream[A] <: {
 *  def flatMap[B](f: A ⇒ Stream[B]): Stream[B]
 * }
 */
package object join {
  import dsl.QFree
  import storage.Storage
  import scala.reflect.ClassTag

  trait StorageModule {
    type Record
    type ReadSettings
    type Cursor <: {
      def hasNext(): Boolean
      def next(): Record
    }
    type Client
    type Stream[A] <: {
      def map[B](f: A ⇒ B): Stream[B]
    }

    type Context
  }

  /**
   * Module extends StorageModule and have implicits values Joiner[Module] and Storage[Module] in scope
   *
   */
  final case class Join[Module <: StorageModule: Joiner: Storage](implicit ctx: Module#Context, client: Module#Client, t: ClassTag[Module]) {
    import org.apache.log4j.Logger

    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Producer-join")

    def join[A](outerQ: QFree[Module#ReadSettings], outerC: String,
                innerQ: Module#Record ⇒ QFree[Module#ReadSettings], innerC: String, resource: String)
                (mapper: (Module#Record, Module#Record) ⇒ A): Module#Stream[A] = {
      val storage = Storage[Module]
      val outer = storage.outer(outerQ, outerC, resource, logger, ctx)(client)
      val relation = storage.inner(innerQ, innerC, resource, logger, ctx)(client)
      Joiner[Module].join[Module#Record, Module#Record, A](outer)(relation)(mapper)
    }
  }

  private[join] trait Joiner[T <: StorageModule] {
    def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])(mapper: (A, B) ⇒ C)
                     (implicit ctx: T#Context): T#Stream[C]
  }

  object Joiner {
    import join.mongo.{ MongoObservable, MongoProcess, MongoAkkaStream }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraAkkaStream }

    /**
     * Performs sequentual join
     */
    implicit object MongoP extends Joiner[MongoProcess] {
      def join[A, B, C](outer: MongoProcess#Stream[A])(relation: A ⇒ MongoProcess#Stream[B])(f: (A, B) ⇒ C)
                       (implicit ctx: MongoProcess#Context): MongoProcess#Stream[C] =
        for {
          id ← outer
          rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _))
        } yield rs
    }

    implicit object MongoO extends Joiner[MongoObservable] {
      override def join[A, B, C](outer: MongoObservable#Stream[A])(relation: A ⇒ MongoObservable#Stream[B])(f: (A, B) ⇒ C)
                                (implicit ctx: MongoObservable#Context): MongoObservable#Stream[C] =
        for {
          id ← outer
          rs ← relation(id).map(f(id, _))
        } yield rs
    }

    implicit object CassandraP extends Joiner[CassandraProcess] {
      override def join[A, B, C](outer: CassandraProcess#Stream[A])(relation: A ⇒ CassandraProcess#Stream[B])(f: (A, B) ⇒ C)
                                (implicit ctx: CassandraProcess#Context): CassandraProcess#Stream[C] = {
        for {
          id ← outer
          rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _))
        } yield rs
      }
    }

    implicit object CassandraO extends Joiner[CassandraObservable] {
      override def join[A, B, C](outer: CassandraObservable#Stream[A])(relation: (A) ⇒ CassandraObservable#Stream[B])(f: (A, B) ⇒ C)
                                (implicit ctx: CassandraObservable#Context): CassandraObservable#Stream[C] =
        for {
          id ← outer
          rs ← relation(id).map(f(id, _))
        } yield rs
    }

    type AkkaSource[x] = akka.stream.scaladsl.Source[x, Unit]

    private def akkaSequentualSource[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B], cmb: (A, B) => C,
                                              system: akka.actor.ActorSystem): AkkaSource[C] =
      outer.map(a => relation(a).map(b => cmb(a, b)))
        .flatten(akka.stream.scaladsl.FlattenStrategy.concat[C])


    implicit object MongoASP extends Joiner[MongoAkkaStream] {
      override def join[A, B, C](outer: Source[A, Unit])(relation: (A) => Source[B, Unit])
                                (mapper: (A, B) => C)
                                (implicit ctx: MongoAkkaStream#Context): MongoAkkaStream#Stream[C] =
      ctx.fold(system => akkaSequentualSource(outer, relation, mapper, system), { ctxData =>
        akkaParallelSource(outer, relation, mapper, ctxData.parallelism) (akka.stream.ActorMaterializer(ctxData.setting)(ctxData.system),
          ctxData.M.asInstanceOf[scalaz.Monoid[C]])
      })
    }

    implicit object CassandraASP extends Joiner[CassandraAkkaStream] {
      override def join[A, B, C](outer: Source[A, Unit])(relation: (A) => Source[B, Unit])
                                (mapper: (A, B) => C)
                                (implicit ctx: CassandraAkkaStream#Context): CassandraAkkaStream#Stream[C] = {
        ctx.fold(system => akkaSequentualSource(outer, relation, mapper, system), { ctxData =>
          akkaParallelSource(outer, relation, mapper, ctxData.parallelism) (akka.stream.ActorMaterializer(ctxData.setting)(ctxData.system),
            ctxData.M.asInstanceOf[scalaz.Monoid[C]])
        })
      }
    }

    private def akkaParallelSource[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B], cmb: (A, B) => C, parallelism: Int)
                                           (implicit mat: akka.stream.ActorMaterializer, M: scalaz.Monoid[C]): AkkaSource[C] = {
      outer.via(Flow[A].mapAsyncUnordered(parallelism) { ids =>
        relation(ids).map(b => cmb(ids, b)).runFold(List[C]())(_ :+ _)
      }).conflate(_.reduce(M.append(_,_))) ((line, list) => M.append(line, list.reduce(M.append(_,_))))
    }

    case class AkkaConcurrentAttributes(setting: akka.stream.ActorMaterializerSettings,
                                        system: akka.actor.ActorSystem,
                                        parallelism: Int,
                                        M: scalaz.Monoid[_])


    def apply[T <: StorageModule: Joiner]: Joiner[T] = implicitly[Joiner[T]]
  }
}
