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

import scalaz.~>
import scalaz.Free.liftFC
import scala.reflect.ClassTag
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap

package object dsl {

  sealed trait StatementOp[T]

  type QFree[A] = scalaz.Free.FreeC[StatementOp, A]

  object mongo {

    import join.mongo.MongoReadSettings
    import com.mongodb.{ DBObject, BasicDBObject }

    type QueryM[T] = scalaz.State[MongoReadSettings, T]

    case class EqOp(q: DBObject) extends StatementOp[MongoReadSettings]

    case class ChainOp(q: DBObject) extends StatementOp[MongoReadSettings]

    case class Sort(q: DBObject) extends StatementOp[MongoReadSettings]

    case class Skip(n: Int) extends StatementOp[MongoReadSettings]

    case class Limit(n: Int) extends StatementOp[MongoReadSettings]

    implicit def f2FreeM(q: _root_.mongo.EqQueryFragment): QFree[MongoReadSettings] = liftFC(EqOp(q.q))

    implicit def c2FreeM(q: _root_.mongo.StatefullQuery): QFree[MongoReadSettings] = liftFC(ChainOp(q.q))

    implicit def sort2FreeM(kv: (String, _root_.mongo.Order.Value)): QFree[MongoReadSettings] = liftFC(Sort(new BasicDBObject(kv._1, kv._2.id)))

    def sort(h: (String, _root_.mongo.Order.Value), t: (String, _root_.mongo.Order.Value)*): QFree[MongoReadSettings] = {
      liftFC(Sort(t.toList.foldLeft(new BasicDBObject(h._1, h._2.id)) { (acc, c) ⇒
        acc.append(c._1, c._2.id)
      }))
    }

    def skip(n: Int): QFree[MongoReadSettings] = liftFC(Skip(n))

    def limit(n: Int): QFree[MongoReadSettings] = liftFC(Limit(n))

    object MongoQueryInterpreter extends (StatementOp ~> QueryM) {
      def apply[T](op: StatementOp[T]): QueryM[T] = op match {
        case EqOp(q) ⇒
          scalaz.State { (in: MongoReadSettings) ⇒
            (in.copy(query = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.query.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case ChainOp(q) ⇒
          scalaz.State { (in: MongoReadSettings) ⇒
            (in.copy(query = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.query.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case Sort(q)  ⇒ scalaz.State { (in: MongoReadSettings) ⇒ (in.copy(sort = Option(q)), in) }
        case Skip(n)  ⇒ scalaz.State { (in: MongoReadSettings) ⇒ (in.copy(skip = Option(n)), in) }
        case Limit(n) ⇒ scalaz.State { (in: MongoReadSettings) ⇒ (in.copy(limit = Option(n)), in) }
      }
    }
  }

  object cassandra {
    import join.cassandra.CassandraReadSettings
    import join.cassandra.CassandraParamValue

    type QueryC[T] = scalaz.State[CassandraReadSettings, T]

    case class CassandraSelect(q: String) extends StatementOp[CassandraReadSettings]
    case class CassandraParam(name: String, v: AnyRef, c: Class[_]) extends StatementOp[CassandraReadSettings]

    /**
     *
     * @param q
     * @return
     */
    def select(q: String): QFree[CassandraReadSettings] = liftFC(CassandraSelect(q))

    /**
     * @tparam T something from java.lang. for correct work with cassandra driver
     */
    def fk[T <: AnyRef](name: String, v: T)(implicit t: ClassTag[T]): QFree[CassandraReadSettings] =
      liftFC(CassandraParam(name, v, t.runtimeClass))

    object CassandraQueryInterpreter extends (StatementOp ~> QueryC) {
      override def apply[A](fa: StatementOp[A]): QueryC[A] = fa match {
        case CassandraSelect(select) ⇒
          scalaz.State { (in: CassandraReadSettings) ⇒ (in.copy(select), in) }
        case CassandraParam(name, v, c) ⇒
          scalaz.State { (in: CassandraReadSettings) ⇒ (in.copy(v = Some(CassandraParamValue(name, v, c))), in) }
      }
    }
  }
}