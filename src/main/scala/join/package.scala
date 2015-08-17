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

  final case class Join[T <: StorageModule: Joiner: Storage](implicit ctx: T#Context, client: T#Client, t: ClassTag[T]) {
    import org.apache.log4j.Logger

    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Producer-join")

    def join[A](leftQ: QFree[T#ReadSettings], lCollection: String,
                rightQ: T#Record ⇒ QFree[T#ReadSettings], rCollection: String,
                resource: String)(f: (T#Record, T#Record) ⇒ A): T#Stream[A] = {

      val storage = Storage[T]
      val outer = storage.outer(leftQ, lCollection, resource, logger, ctx)(client)
      val relation = storage.inner(rightQ, rCollection, resource, logger, ctx)(client)

      Joiner[T].join[T#Record, T#Record, A](outer)(relation)(f)
    }
  }

  private[join] trait Joiner[T <: StorageModule] {
    def join[A, B, C](outer: T#Stream[A])(relation: A ⇒ T#Stream[B])(f: (A, B) ⇒ C): T#Stream[C]
  }

  object Joiner {
    import rx.lang.scala.Observable
    import join.mongo.{ MongoObservable, MongoProcess, MongoAkkaStream }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraAkkaStream }

    /**
     * Performs sequentual join
     */
    implicit object MongoP extends Joiner[MongoProcess] {
      def join[A, B, C](outer: MongoProcess#Stream[A])(relation: A ⇒ MongoProcess#Stream[B])(f: (A, B) ⇒ C): MongoProcess#Stream[C] =
        for { id ← outer; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _)) } yield rs
    }

    /**
     * Runs each inner query in parallel utilized executor
     */
    implicit object MongoO extends Joiner[MongoObservable] {
      override def join[A, B, C](outer: MongoObservable#Stream[A])(relation: A ⇒ MongoObservable#Stream[B])(f: (A, B) ⇒ C): MongoObservable#Stream[C] =
        for { id ← outer; rs ← relation(id).map(f(id, _)) } yield rs
    }

    /**
     * Performs sequentual join
     */
    implicit object CassandraP extends Joiner[CassandraProcess] {
      override def join[A, B, C](outer: CassandraProcess#Stream[A])(relation: A ⇒ CassandraProcess#Stream[B])(f: (A, B) ⇒ C): CassandraProcess#Stream[C] = {
        for { id ← outer; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _))} yield rs
      }
    }

    /**
     * Runs each inner query in parallel utilized executor
     */
    implicit object CassandraO extends Joiner[CassandraObservable] {
      override def join[A, B, C](outer: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): Observable[C] =
        for { id ← outer; rs ← relation(id).map(f(id, _)) } yield rs
    }


    /**
     * Performs sequentual join
     */
    implicit object MongoAS extends Joiner[MongoAkkaStream] {
      override def join[A, B, C](outer: MongoAkkaStream#Stream[A])
                                (relation: (A) => MongoAkkaStream#Stream[B])(f: (A, B) => C): MongoAkkaStream#Stream[C] =
        akkaFlattenSource[A,B,C](outer, relation, f)
    }

    /**
     * Performs sequentual join
     */
    implicit object CassandraAS extends Joiner[CassandraAkkaStream] {
      override def join[A, B, C](outer: CassandraAkkaStream#Stream[A])
                                (relation: (A) => CassandraAkkaStream#Stream[B])(f: (A, B) => C): CassandraAkkaStream#Stream[C] =
        akkaFlattenSource[A,B,C](outer, relation, f)
    }

    type AkkaSource[x] = akka.stream.scaladsl.Source[x, Unit]

    private def akkaFlattenSource[A, B, C](outer: AkkaSource[A], relation: (A) => AkkaSource[B], cmb: (A, B) => C): AkkaSource[C] =
      outer.map(a => relation(a).map(b => cmb(a, b))).flatten(akka.stream.scaladsl.FlattenStrategy.concat[C])

    /**
     *
     * @tparam T
     * @return
     */
    def apply[T <: StorageModule: Joiner]: Joiner[T] = implicitly[Joiner[T]]
  }
}