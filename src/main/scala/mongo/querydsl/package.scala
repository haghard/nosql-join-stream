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

package mongo

import java.util.Date

package object querydsl {

  type Effect = StringBuilder ⇒ Unit
  type Par = (String, Action)
  type KVS = (String, TraversableOnce[Par])

  object Action {
    import scala.xml.Utility.{ escape ⇒ esc }

    private[querydsl] def s(s: String): Action = Action(sb ⇒ sb append s)

    private[querydsl] def value[T](s: T): Action = Action(sb ⇒ sb append s)

    private[querydsl] def escape(s: String): Action = Action(sb ⇒ sb append esc(s))

    private[querydsl] def intersperse(delim: Action)(as: TraversableOnce[Action]) = Action { sb ⇒
      var between = false
      as foreach { a ⇒
        if (between) {
          delim(sb)
          a(sb)
        } else {
          a(sb)
          between = true
        }
      }
    }

    private[querydsl] def entry(key: String, value: Action): Action =
      literal(key) ++ s(" : ") ++ value

    private[querydsl] def obj(entries: TraversableOnce[Par]): Action =
      s("{ ") ++ intersperse(s(", "))(entries.map(p ⇒ entry(p._1, p._2))) ++ s(" }")

    private[querydsl] def list(values: TraversableOnce[Action]): Action =
      s("[") ++ intersperse(s(", "))(values) ++ s("]")

    private[querydsl] def nestedMap(entries: TraversableOnce[KVS]): Action = Action { sb ⇒
      var start = true
      entries foreach { en ⇒
        if (start) { start = false; s("{ ")(sb) }
        else s(" , ")(sb)
        (literal(en._1) ++ s(" : ") ++ obj(en._2))(sb)
      }
      if (!start) s(" } ")(sb)
    }
  }

  final case class Action(f: Effect) extends Effect {
    def apply(sb: StringBuilder) = f(sb)

    def ++(other: Action) = Action { sb ⇒
      apply(sb)
      other(sb)
    }

    override def toString: String = {
      val sb = new StringBuilder
      apply(sb)
      sb.toString
    }
  }

  import Action._

  def literal(l: String): Action = s("\"") ++ escape(l) ++ s("\"")

  def NestedMap(entries: KVS*): Action = nestedMap(entries)

  def List(values: Action*): Action = list(values)

  def Obj(entries: Par*): Action = obj(entries)

  implicit def str2LiteralAction(s: String) = literal(s)

  implicit def value2Action[T: Types](n: T): Action = {
    val item = n match {
      case date: Date ⇒ literal(formatter.format(date))
      case other      ⇒ n
    }
    value(item)
  }

  implicit def op2Name[T <: MqlOp](operation: T): String = operation.op
}
