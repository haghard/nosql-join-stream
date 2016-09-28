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

import scala.util.Try
import scala.annotation.tailrec
import com.mongodb.{ BasicDBObject, MongoException }
import scala.util.parsing.combinator.{ JavaTokenParsers, PackratParsers }

package object mqlparser {

  sealed trait MqlValue extends MqlExpression {
    type T <: AnyRef
    def value: T
  }

  case class MqlString(override val value: String) extends MqlValue {
    type T = String
  }

  case class MqlDouble(override val value: java.lang.Double) extends MqlValue {
    type T = java.lang.Double
  }

  case class MqlInt(override val value: java.lang.Integer) extends MqlValue {
    type T = java.lang.Integer
  }

  case class MqlBool(override val value: java.lang.Boolean) extends MqlValue {
    type T = java.lang.Boolean
  }

  case class MqlIntArray(override val value: Array[Int]) extends MqlValue {
    type T = Array[Int]
  }

  case class MqlDoubleArray(override val value: Array[Double]) extends MqlValue {
    type T = Array[Double]
  }

  case class MqlStringArray(override val value: Array[String]) extends MqlValue {
    type T = Array[String]
  }

  case class MqlISODate(override val value: java.util.Date) extends MqlValue {
    type T = java.util.Date
  }

  case class MqlRight(val operation: String, val value: MqlValue) extends MqlExpression

  implicit class StringExt(val str: String) {
    def clean = str.replace("\"", "")
  }

  private[mqlparser] final class MongoLangParser extends JavaTokenParsers with PackratParsers {
    import scala.collection.JavaConversions.seqAsJavaList

    type P[T] = PackratParser[T]

    private val UNNECESSARY_NAME_OP = "$eq"

    private val int = "(-?[0-9]+)".r
    private val double = "(-?[0-9]*[.][0-9]+)".r
    private val bool = """(true|false)""".r
    private val boolLiteral: P[String] = regex(bool)

    lazy val rangeQueryOperators: P[MqlOp] = (""""$gte"""" | """"$gt"""" | """"$lte"""" | """"$lt"""" | """":"""" | """"$ne"""" | """"$not"""" /*| """"$eq""""*/ ) ^^ {
      case """"$gte"""" ⇒ $gte()
      case """"$gt""""  ⇒ $gt()
      case """"$lt""""  ⇒ $lt()
      case """"$lte"""" ⇒ $lte()

      case """"$ne""""  ⇒ $ne()
      case """":""""    ⇒ $eq()
      //case """"$eq""""  ⇒ $eq()
      case f            ⇒ throw new UnsupportedOperationException(s"unsupported rangeQueryOperator $f")
    }

    lazy val setQueryOperators: P[MqlOp] = (""""$in"""" | """"$all"""" | """"$nin"""") ^^ {
      case """"$in""""  ⇒ $in()
      case """"$nin"""" ⇒ $nin()
      case """"$all"""" ⇒ $all()
      case f            ⇒ throw new UnsupportedOperationException(s"unsupported field ${f} ")
    }

    //TODO: support for other boolean ops "$not" | $exists
    lazy val booleanOperators: P[String] = (""""$or"""" | """"$and"""") ^^ { case v ⇒ v }

    lazy val strArray: P[MqlStringArray] = "[" ~> stringLiteral ~ rep("," ~> stringLiteral) <~ "]" ^^ {
      case start ~ other ⇒ {
        if (other.isEmpty) {
          MqlStringArray(Array(start.clean))
        } else {
          MqlStringArray(other.map({ _.clean }).::(start.clean).toArray)
        }
      }
    }

    lazy val intArray: P[MqlIntArray] = "[" ~> wholeNumber ~ rep("," ~> wholeNumber) <~ "]" ^^ {
      case start ~ other ⇒ {
        if (other.isEmpty) {
          MqlIntArray(Array(start.toInt))
        } else {
          MqlIntArray(other.map({ _.toInt }).::(start.toInt).toArray)
        }
      }
    }

    lazy val doubleArray: P[MqlDoubleArray] = "[" ~> floatingPointNumber ~ rep("," ~> floatingPointNumber) <~ "]" ^^ {
      case start ~ other ⇒ {
        if (other.isEmpty) {
          MqlDoubleArray(Array(start.toDouble))
        } else {
          MqlDoubleArray(other.map({ _.toDouble }).::(start.toDouble).toArray)
        }
      }
    }

    lazy val array: P[MqlValue] = intArray | doubleArray | strArray

    lazy val selectorValue: P[MqlValue] = (floatingPointNumber | wholeNumber | boolLiteral | stringLiteral | array) ^^ {
      case value ⇒ value match {
        case v: String ⇒
          v.clean match {
            case double(v) ⇒ MqlDouble(v.toDouble)
            case int(v)    ⇒ MqlInt(v.toInt)
            case bool(v)   ⇒ MqlBool(v.toBoolean)
            case str       ⇒ Try(MqlISODate(formatter parse str)).getOrElse(MqlString(v.clean))
          }
        case v: MqlValue ⇒ v
      }
    }

    /**
     *
     * age or user.age
     *
     */
    lazy val field: P[String] = stringLiteral ~ rep("." ~> stringLiteral) ^^ {
      case start ~ other ⇒ other match {
        case Nil ⇒ start
        case _   ⇒ start + "." + other.mkString(".")
      }
    }

    /**
     *
     *
     * $gt : 45
     *
     */
    lazy val selector: P[MqlRight] = (rangeQueryOperators | setQueryOperators) ~ ":" ~ selectorValue ^^ {
      case operation ~ sep ~ value ⇒
        MqlRight(operation.op.clean, value)
    }

    /**
     * $gt: 39, $lt:42
     *
     */
    lazy val selectors: P[List[MqlRight]] = selector ~ rep("," ~> selector) ^^ {
      case head ~ tail ⇒ tail.+:(head)
    }

    lazy val selectorsOrValue: P[List[MqlRight]] = (selectorValue | "{" ~> selectors <~ "}") ^^ {
      case t ⇒ t match {
        case v: MqlValue                  ⇒ List(MqlRight("$eq", v))
        case s: List[MqlRight] @unchecked ⇒ s
      }
    }

    lazy val selectorElements: P[BasicDBObject] = "{" ~> fieldSelectors ~ rep("," ~> fieldSelectors) <~ "}" ^^ {
      case first ~ rest ⇒
        val start = toDBObject(first)
        rest.foldLeft(start) { (acc, c) ⇒
          if (c._2.get(UNNECESSARY_NAME_OP) != null) acc.append(c._1, c._2.get(UNNECESSARY_NAME_OP))
          else acc.append(c._1, c._2)
        }
    }

    lazy val fieldSelectors: P[(String, BasicDBObject)] = field ~ ":" ~ selectorsOrValue ^^ {
      case field ~ sep0 ~ right ⇒ {
        val start: MqlRight = right.head
        val dbObject = new BasicDBObject(start.operation, start.value.value)
        val anonymousDbObject = loop(right.tail, dbObject)
        (field.clean → anonymousDbObject)
      }
    }

    @tailrec
    private def loop(predicates: List[MqlRight], startDbObj: BasicDBObject): BasicDBObject =
      predicates match {
        case Nil          ⇒ startDbObj
        case head :: tail ⇒ loop(tail, startDbObj.append(head.operation, head.value.value))
      }

    private def toDBObject(p: (String, BasicDBObject)) =
      if (p._2.get(UNNECESSARY_NAME_OP) != null) new BasicDBObject(p._1, p._2.get(UNNECESSARY_NAME_OP))
      else new BasicDBObject(p._1, p._2)

    lazy val arrayElement: P[BasicDBObject] = (selectorElements | logicalSelector) ^^ { case obj ⇒ obj }

    lazy val selectorsArray: P[java.util.List[BasicDBObject]] = arrayElement ~ rep("," ~> arrayElement) ^^ {
      case first ~ other ⇒
        if (other.isEmpty) java.util.Arrays.asList(first)
        else seqAsJavaList(first :: other)
    }

    lazy val logicalSelector: P[BasicDBObject] = "{" ~> booleanOperators ~ ":" ~ "[" ~ selectorsArray <~ "]" <~ "}" ^^ {
      case l ~ sep0 ~ sep2 ~ c ⇒ new BasicDBObject(l.clean, c)
    }

    lazy val query: P[BasicDBObject] = (selectorElements | logicalSelector)

    def parse(expressionLine: String): BasicDBObject = {
      import scala.util.parsing.input.CharSequenceReader
      val parser: PackratParser[BasicDBObject] = phrase(query)
      parser(new PackratReader(new CharSequenceReader(expressionLine))) match {
        case Success(dbObject, _) ⇒ dbObject
        case Failure(msg, _)      ⇒ throw new MongoException(s"Failure: query parse error: $msg")
      }
    }
  }

  object MqlParser {
    def apply() = new MongoLangParser
  }
}
