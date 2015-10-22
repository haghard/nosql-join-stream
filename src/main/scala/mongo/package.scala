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

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConversions._
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import com.mongodb.{ TagSet, DBObject, BasicDBObject }

package object mongo {

  object ReadPreference extends Enumeration {
    val Primary, Secondary, Nearest = Value
  }

  implicit def mrpToReadPreference(rp: ReadPreference.Value) = ReadPreference(rp)

  case class ReadPreference(pref: ReadPreference.Value, preferred: Boolean = false, tag: List[TagSet] = Nil) {
    import scala.collection.JavaConverters._
    import com.mongodb.{ ReadPreference ⇒ NativeReadPreference }

    def preferred(b: Boolean): ReadPreference = copy(preferred = true)

    def tags(ts: List[TagSet]): ReadPreference = copy(tag = tag ++ ts)

    private[mongo] val asMongoDbReadPreference: NativeReadPreference = this match {
      case ReadPreference(ReadPreference.Nearest, _, Nil)            ⇒ NativeReadPreference.nearest()
      case ReadPreference(ReadPreference.Nearest, _, h :: Nil)       ⇒ NativeReadPreference.nearest(h)
      case ReadPreference(ReadPreference.Nearest, _, h :: t)         ⇒ NativeReadPreference.nearest((h :: t).asJava)
      case ReadPreference(ReadPreference.Primary, true, Nil)         ⇒ NativeReadPreference.primaryPreferred()
      case ReadPreference(ReadPreference.Primary, true, h :: Nil)    ⇒ NativeReadPreference.primaryPreferred(h)
      case ReadPreference(ReadPreference.Primary, true, h :: t)      ⇒ NativeReadPreference.primaryPreferred((h :: t).asJava)
      case ReadPreference(ReadPreference.Primary, false, Nil)        ⇒ NativeReadPreference.primary()
      case ReadPreference(ReadPreference.Secondary, true, Nil)       ⇒ NativeReadPreference.secondaryPreferred()
      case ReadPreference(ReadPreference.Secondary, true, h :: Nil)  ⇒ NativeReadPreference.secondaryPreferred(h)
      case ReadPreference(ReadPreference.Secondary, true, h :: t)    ⇒ NativeReadPreference.secondaryPreferred((h :: t).asJava)
      case ReadPreference(ReadPreference.Secondary, false, Nil)      ⇒ NativeReadPreference.secondary()
      case ReadPreference(ReadPreference.Secondary, false, h :: Nil) ⇒ NativeReadPreference.secondary(h)
      case ReadPreference(ReadPreference.Secondary, false, h :: t)   ⇒ NativeReadPreference.secondary((h :: t).asJava)
      case ReadPreference(ReadPreference.Primary, false, nonEmpty)   ⇒ sys.error("not supported")
    }
  }

  sealed trait QueryBuilder {
    def q: BasicDBObject
  }

  object Order extends Enumeration {
    val Ascending = Value(1)
    val Descending = Value(-1)
  }

  object FetchMode extends Enumeration {
    type Type = Value
    val One, Batch = Value
  }

  trait QueryDsl extends scalaz.syntax.Ops[StatefullQuery] {

    def field: String

    def nested: Option[BasicDBObject]

    private def update[T](v: T, op: String) =
      Option(nested.fold(new BasicDBObject(op, v))(_.append(op, v)))

    private def update[T](v: java.lang.Iterable[T], op: String) =
      Option(nested.fold(new BasicDBObject(op, v))(_.append(op, v)))

    def $eq[T: Types](v: T) = EqQueryFragment(new BasicDBObject(field, v))
    def $gt[T: Types](v: T) = self.copy(field, update(v, "$gt"))
    def $gte[T: Types](v: T) = self.copy(field, update(v, "$gte"))
    def $lt[T: Types](v: T) = self.copy(field, update(v, "$lt"))
    def $lte[T: Types](v: T) = self.copy(field, update(v, "$lte"))
    def $ne[T: Types](v: T) = self.copy(field, update(v, "$ne"))
    def $in[T: Types](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$in"))
    def $all[T: Types](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$all"))
    def $nin[T: Types](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$nin"))
  }

  case class EqQueryFragment(override val q: BasicDBObject) extends QueryBuilder

  case class StatefullQuery(val field: String, val nested: Option[BasicDBObject]) extends QueryDsl with QueryBuilder {
    override val self = this
    override def q = new BasicDBObject(field, nested.fold(new BasicDBObject())(x ⇒ x))
    override def toString() = q.toString
  }

  case class ConjunctionQuery(cs: TraversableOnce[QueryBuilder]) extends QueryBuilder {
    override def q = new BasicDBObject("$and", cs./:(new java.util.ArrayList[DBObject]()) { (arr, c) ⇒
      (arr add c.q)
      arr
    })
    override def toString() = q.toString
  }

  case class DisjunctionQuery(cs: TraversableOnce[QueryBuilder]) extends QueryBuilder {
    override def q = new BasicDBObject("$or", cs./:(new java.util.ArrayList[DBObject]()) { (arr, c) ⇒
      (arr add c.q)
      arr
    })
    override def toString() = q.toString
  }

  implicit def f2b(field: String) = StatefullQuery(field, None)

  def &&(bs: QueryBuilder*) = ConjunctionQuery(bs)
  def ||(bs: QueryBuilder*) = DisjunctionQuery(bs)

  //Supported types
  sealed trait Types[T]
  implicit val intV = new Types[Int] {}
  implicit val longV = new Types[Long] {}
  implicit val doubleV = new Types[Double] {}
  implicit val stringV = new Types[String] {}
  implicit val booleanV = new Types[Boolean] {}
  implicit val dateV = new Types[Date] {}

  trait MqlExpression

  def formatter() = new SimpleDateFormat("dd MMM yyyy hh:mm:ss:SSS a z")

  sealed trait MqlOp extends MqlExpression {
    def op: String
  }

  case class $gt(override val op: String = "$gt") extends MqlOp

  case class $gte(override val op: String = "$gte") extends MqlOp

  case class $lt(override val op: String = "$lt") extends MqlOp

  case class $lte(override val op: String = "$lte") extends MqlOp

  case class $eq(override val op: String = "$eq") extends MqlOp

  //set operators
  case class $in(override val op: String = "$in") extends MqlOp
  case class $all(override val op: String = "$all") extends MqlOp
  case class $nin(override val op: String = "$nin") extends MqlOp

  //boolean operators
  case class $and(override val op: String = "$and") extends MqlOp
  case class $or(override val op: String = "$or") extends MqlOp
  case class $ne(override val op: String = "$ne") extends MqlOp

  /**
   *
   * @param name
   */
  final class NamedThreadFactory(val name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread-"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    def newThread(r: Runnable) =
      new Thread(this.group, r, namePrefix + this.threadNumber.getAndIncrement(), 0L)
  }
}