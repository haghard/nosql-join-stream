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

  abstract class MongoTypeReader[T](implicit val classTag: ClassTag[T]) {
    def convert(v: AnyRef): T
  }

  object MongoTypeReader {
    def apply[T: MongoTypeReader]: MongoTypeReader[T] = implicitly[MongoTypeReader[T]]

    implicit val fromInt = new MongoTypeReader[Int] {
      def convert(value: AnyRef) = value.asInstanceOf[Int]
    }

    implicit val fromDouble = new MongoTypeReader[Double] {
      def convert(value: AnyRef) = value.asInstanceOf[Double]
    }

    implicit val fromLong = new MongoTypeReader[Long] {
      def convert(value: AnyRef) = value.asInstanceOf[Long]
    }

    implicit val fromString = new MongoTypeReader[String] {
      def convert(value: AnyRef) = value.asInstanceOf[String]
    }

    implicit val fromDate = new MongoTypeReader[java.util.Date] {
      def convert(value: AnyRef) = value.asInstanceOf[java.util.Date]
    }

    implicit val fromBoolean = new MongoTypeReader[Boolean] {
      def convert(value: AnyRef) = value.asInstanceOf[Boolean]
    }
  }

  implicit class MongoTypeReaderOps(val fieldObject: AnyRef) {
    def as[T: MongoTypeReader]: T = (implicitly[MongoTypeReader[T]] convert fieldObject)
  }

  trait MongoRecordParser[A] {
    def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[A]
  }

  implicit val intP = new MongoRecordParser[Int] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Int] = {
      Try(obj.get(fields(ind)).asInstanceOf[Int]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val doubleP = new MongoRecordParser[Double] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Double] = {
      Try(obj.get(fields(ind)).asInstanceOf[Double]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val longP = new MongoRecordParser[Long] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[Long] = {
      Try(obj.get(fields(ind)).asInstanceOf[Long]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val stringP = new MongoRecordParser[String] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[String] = {
      Try(obj.get(fields(ind)).asInstanceOf[String]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val dateP = new MongoRecordParser[java.util.Date] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[java.util.Date] = {
      Try(obj.get(fields(ind)).asInstanceOf[java.util.Date]).map(Some(_)).getOrElse(None)
    }
  }

  implicit val nilParser: MongoRecordParser[HNil] = new MongoRecordParser[HNil] {
    override def apply(obj: DBObject, fields: Vector[String], ind: Int): Option[HNil] =
      if (fields.size >= ind) Some(HNil) else None
  }

  implicit def hConsParserMongo[H: MongoRecordParser, T <: HList: MongoRecordParser]: MongoRecordParser[H :: T] =
    new MongoRecordParser[H :: T] {
      def apply(obj: DBObject, fields: Vector[String], acc: Int): Option[H :: T] = {
        fields match {
          case h +: rest ⇒
            for {
              head ← implicitly[MongoRecordParser[H]].apply(obj, fields, acc)
              tail ← implicitly[MongoRecordParser[T]].apply(obj, fields, acc + 1)
            } yield head :: tail
        }
      }
    }

  //case class parser
  implicit def caseClassParserMongo[A, R <: HList](implicit Gen: Generic.Aux[A, R], parser: MongoRecordParser[R]) =
    new MongoRecordParser[A] {
      def apply(obj: DBObject, fields: Vector[String], acc: Int): Option[A] = {
        parser(obj, fields, acc).map { hlist ⇒ Gen.from(hlist) }
      }
    }

  implicit class MongoReaderOps(val obj: DBObject) {
    def as[T](implicit parser: MongoRecordParser[T], tag: ClassTag[T]): Option[T] = {
      val fields = tag.runtimeClass.getDeclaredFields.map(_.getName).toVector
      parser(obj, fields, 0)
    }
  }

  trait CassandraRecordReader[A] {
    def apply(row: Row, fields: Vector[String], ind: Int): Option[A]
  }

  implicit val cInt = new CassandraRecordReader[Int] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[Int] = {
      Try(row.getInt(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cDouble = new CassandraRecordReader[Double] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[Double] = {
      Try(row.getDouble(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cLong = new CassandraRecordReader[Long] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[Long] = {
      Try(row.getLong(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cString = new CassandraRecordReader[String] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[String] = {
      Try(row.getString(fields(ind))).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cDate = new CassandraRecordReader[java.util.Date] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[java.util.Date] = {
      Try(new java.util.Date(row.getDate(fields(ind)).getMillisSinceEpoch)).map(Some(_)).getOrElse(None)
    }
  }

  implicit val cNilParser: CassandraRecordReader[HNil] = new CassandraRecordReader[HNil] {
    override def apply(row: Row, fields: Vector[String], ind: Int): Option[HNil] =
      if (fields.size >= ind) Some(HNil) else None
  }

  implicit def hConsParserCassandra[H: CassandraRecordReader, T <: HList: CassandraRecordReader]: CassandraRecordReader[H :: T] =
    new CassandraRecordReader[H :: T] {
      def apply(row: Row, fields: Vector[String], acc: Int): Option[H :: T] = {
        fields match {
          case h +: rest ⇒
            for {
              head ← implicitly[CassandraRecordReader[H]].apply(row, fields, acc)
              tail ← implicitly[CassandraRecordReader[T]].apply(row, fields, acc + 1)
            } yield head :: tail
        }
      }
    }

  //case class parser
  implicit def caseClassParserCassandra[A, R <: HList](implicit Gen: Generic.Aux[A, R], parser: CassandraRecordReader[R]) =
    new CassandraRecordReader[A] {
      def apply(obj: Row, fields: Vector[String], acc: Int): Option[A] = {
        parser(obj, fields, acc).map { hlist ⇒ Gen.from(hlist) }
      }
    }

  implicit class CassandraRecordOps(val row: Row) {
    def as[T](implicit parser: CassandraRecordReader[T], tag: ClassTag[T]): Option[T] = {
      val fields = tag.runtimeClass.getDeclaredFields.map(_.getName).toVector
      parser(row, fields, 0)
    }
  }
}