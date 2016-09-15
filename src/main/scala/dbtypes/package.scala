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

package object dbtypes {

  trait FromJavaConverter[T] {
    def convert(v: AnyRef): T
  }

  object FromJavaConverter {
    implicit val fromInt = new FromJavaConverter[Int] {
      def convert(n: AnyRef) = {
        n.asInstanceOf[java.lang.Integer].asInstanceOf[Int]
      }
    }

    implicit val fromDouble = new FromJavaConverter[Double] {
      def convert(n: AnyRef) =
        n.asInstanceOf[java.lang.Double].asInstanceOf[Double]
    }

    implicit val fromLong = new FromJavaConverter[Long] {
      def convert(n: AnyRef) =
        n.asInstanceOf[java.lang.Long].asInstanceOf[Long]
    }

    implicit val fromString = new FromJavaConverter[String] {
      def convert(n: AnyRef) = n.asInstanceOf[String]
    }

    implicit val fromDate = new FromJavaConverter[java.util.Date] {
      def convert(n: AnyRef) = n.asInstanceOf[java.util.Date]
    }

    implicit val fromBoolean = new FromJavaConverter[java.lang.Boolean] {
      def convert(n: AnyRef) = n.asInstanceOf[Boolean]
    }
  }
}
