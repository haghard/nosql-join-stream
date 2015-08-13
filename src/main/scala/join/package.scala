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
 *
 * Based on idea from: http://io.pellucid.com/blog/abstract-algebraic-data-type
 */
package object join {
  import dsl.QFree
  import storage.Storage
  import scala.reflect.ClassTag
  import java.util.concurrent.ExecutorService

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
      def flatMap[B](f: A ⇒ Stream[B]): Stream[B]
    }
  }

  final case class Join[T <: StorageModule: Joiner: Storage](implicit pool: ExecutorService, client: T#Client, t: ClassTag[T]) {
    import org.apache.log4j.Logger

    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Producer-join")

    def join[A](leftQ: QFree[T#ReadSettings], lCollection: String,
                rightQ: T#Record ⇒ QFree[T#ReadSettings], rCollection: String,
                resource: String)(f: (T#Record, T#Record) ⇒ A): T#Stream[A] = {

      val storage = Storage[T]
      val outer = storage.outerR(leftQ, lCollection, resource, logger, pool)(client)
      val relation = storage.innerR(rightQ, rCollection, resource, logger, pool)(client)

      Joiner[T].join[T#Record, T#Record, A](outer)(relation)(f)
    }
  }

  private[join] trait Joiner[T <: StorageModule] {
    def join[A, B, C](l: T#Stream[A])(relation: A ⇒ T#Stream[B])(f: (A, B) ⇒ C): T#Stream[C]
  }

  object Joiner {
    import rx.lang.scala.Observable
    import join.mongo.{ MongoObservable, MongoProcess }
    import join.cassandra.{ CassandraObservable, CassandraProcess }

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

    def apply[T <: StorageModule: Joiner]: Joiner[T] = implicitly[Joiner[T]]
  }
}