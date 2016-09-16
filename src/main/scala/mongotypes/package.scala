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

import com.datastax.driver.core.Row
import com.mongodb.DBObject
import shapeless._

import scala.reflect.ClassTag
import scala.util.Try

package object dbtypes {

  /*
  def fromAny[T](parser: AnyRef => T) = new MongoTypeEncoder[T] {
    override def convert(x: AnyRef): T = parser(x)
  }

  implicit val fromInt2 = fromAny[Int](_.asInstanceOf[Int])
  implicit val fromDouble2 = fromAny[Double](_.asInstanceOf[Double])
  implicit val fromLong2 = fromAny[Long](_.asInstanceOf[Long])
  implicit val fromString2 = fromAny[String](_.asInstanceOf[String])
  implicit val fromDate2 = fromAny[java.util.Date](_.asInstanceOf[java.util.Date])
  implicit val fromBoolean2 = fromAny[Boolean](_.asInstanceOf[Boolean])
  */

  abstract class MongoTypeEncoder[T](implicit val classTag: ClassTag[T]) {
    def convert(v: AnyRef): T
  }

  object MongoTypeEncoder {
    def apply[T: MongoTypeEncoder]: MongoTypeEncoder[T] = implicitly[MongoTypeEncoder[T]]

    implicit val fromInt = new MongoTypeEncoder[Int] {
      def convert(value: AnyRef) = value.asInstanceOf[Int]
    }

    implicit val fromDouble = new MongoTypeEncoder[Double] {
      def convert(value: AnyRef) = value.asInstanceOf[Double]
    }

    implicit val fromLong = new MongoTypeEncoder[Long] {
      def convert(value: AnyRef) = value.asInstanceOf[Long]
    }

    implicit val fromString = new MongoTypeEncoder[String] {
      def convert(value: AnyRef) = value.asInstanceOf[String]
    }

    implicit val fromDate = new MongoTypeEncoder[java.util.Date] {
      def convert(value: AnyRef) = value.asInstanceOf[java.util.Date]
    }

    implicit val fromBoolean = new MongoTypeEncoder[Boolean] {
      def convert(value: AnyRef) = value.asInstanceOf[Boolean]
    }
  }

  implicit class MongoTypeEncoderOps(val fieldObject: AnyRef) {
    def as[T: MongoTypeEncoder]: T = (implicitly[MongoTypeEncoder[T]] convert fieldObject)
  }

  trait MongoObjectParser[A] {
    def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[A]
  }


  implicit val intP = new MongoObjectParser[Int] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Int] = {
      Try(obj.get(fields(ind)).asInstanceOf[Int]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val doubleP = new MongoObjectParser[Double] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Double] = {
      Try(obj.get(fields(ind)).asInstanceOf[Double]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val longP = new MongoObjectParser[Long] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Long] = {
      Try(obj.get(fields(ind)).asInstanceOf[Long]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val stringP = new MongoObjectParser[String] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[String] = {
      Try(obj.get(fields(ind)).asInstanceOf[String]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val dateP = new MongoObjectParser[java.util.Date] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[java.util.Date] = {
      println("data " + ind + ":" + fields(ind))
      Try(obj.get(fields(ind)).asInstanceOf[java.util.Date]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val nilParser: MongoObjectParser[HNil] = new MongoObjectParser[HNil] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[HNil] =
      if (fields.size >= ind) Some(HNil) else None
  }

  implicit def hConsParserMongo[H: MongoObjectParser, T <: HList : MongoObjectParser]: MongoObjectParser[H :: T] =
    new MongoObjectParser[H :: T] {
      def apply(obj: DBObject, fields: Vector[String], acc: Int): Option[H :: T] = {
        fields match {
          case h +: rest =>
            for {
              head <- implicitly[MongoObjectParser[H]].apply(obj, fields, acc)
              tail <- implicitly[MongoObjectParser[T]].apply(obj, fields, acc + 1)
            } yield head :: tail
        }
      }
    }

  //case class parser
  implicit def caseClassParserMongo[A, R <: HList](implicit Gen: Generic.Aux[A, R], parser: MongoObjectParser[R]) =
    new MongoObjectParser[A] {
      def apply(obj: DBObject, fields: Vector[String], acc: Int): Option[A] = {
        parser(obj, fields, acc).map { hlist => Gen.from(hlist) }
      }
    }


  implicit class MongoEncoderOps(val obj: DBObject) {
    def as[T](implicit parser: MongoObjectParser[T], tag: ClassTag[T]): Option[T] = {
      val fields = tag.runtimeClass.getDeclaredFields.map(_.getName).toVector
      parser(obj, fields, 0)
    }
  }

  trait CassandraObjectParser[A] {
    def apply(obj: Row, fields: Vector[String], ind: Int): Option[A]
  }


  implicit val cInt = new CassandraObjectParser[Int] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[Int] = {
      Try(obj.getInt(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cDouble = new CassandraObjectParser[Double] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[Double] = {
      Try(obj.getDouble(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cLong = new CassandraObjectParser[Long] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[Long] = {
      Try(obj.getLong(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cString = new CassandraObjectParser[String] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[String] = {
      Try(obj.getString(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cDate = new CassandraObjectParser[java.util.Date] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[java.util.Date] = {
      Try(new java.util.Date(obj.getDate(fields(ind)).getMillisSinceEpoch)).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cNilParser: CassandraObjectParser[HNil] = new CassandraObjectParser[HNil] {
    override def apply(obj: Row, fields: Vector[String], ind: Int): Option[HNil] =
      if (fields.size >= ind) Some(HNil) else None
  }

  implicit def hConsParserCassandra[H: CassandraObjectParser, T <: HList : CassandraObjectParser]: CassandraObjectParser[H :: T] =
    new CassandraObjectParser[H :: T] {
      def apply(obj: Row, fields: Vector[String], acc: Int): Option[H :: T] = {
        fields match {
          case h +: rest =>
            for {
              head <- implicitly[CassandraObjectParser[H]].apply(obj, fields, acc)
              tail <- implicitly[CassandraObjectParser[T]].apply(obj, fields, acc + 1)
            } yield head :: tail
        }
      }
    }

  //case class parser
  implicit def caseClassParserCassandra[A, R <: HList](implicit Gen: Generic.Aux[A, R], parser: CassandraObjectParser[R]) =
    new CassandraObjectParser[A] {
      def apply(obj: Row, fields: Vector[String], acc: Int): Option[A] = {
        parser(obj, fields, acc).map { hlist => Gen.from(hlist) }
      }
    }


  implicit class CassandraEncoderOps(val obj: Row) {
    def as[T](implicit parser: CassandraObjectParser[T], tag: ClassTag[T]): Option[T] = {
      val fields = tag.runtimeClass.getDeclaredFields.map(_.getName).toVector
      parser(obj, fields, 0)
    }
  }

}