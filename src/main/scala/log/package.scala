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

import join.StorageModule
import storage.Storage

import scala.reflect.ClassTag

package object eventlog {

  /**
   * Based on akka-cassandra-persistence schema
   * 5000000l - default PartitionSize from akka-cassandra-persistence
   *
   * Partition:0
   *
   * +-------+--------+-------+
   * |0:body |1: body |2: body|
   * +----+-------+--------+-------+
   * |a:0 |xxx    |xxx     |xxx    |
   * +----+-------+--------+-------+
   *
   * Partition:1
   *
   * +----------------+---------------+--------------+
   * |50000001:body   |50000002:body  |5000003 :body |
   * +---+----------------+---------------+--------------+
   * |a:1|xxx             |xxx            |xxx            |
   * +---+----------------+---------------+--------------+
   *
   * Partition:2
   *
   * +-------+--------+--------+
   * |0:body |1: body |2 :body |
   * +---+-------+--------+--------+
   * |b:0|xxx    |xxx     |xxx     |
   * +---+-------+--------+--------+
   *
   * Partition key - (persistence_id, partition_nr)
   * Clustering key - sequence_nr
   *
   * Log with iterator that goes across partition_nr for specific persistence_id
   *
   */
  case class Log[M <: StorageModule: Storage](implicit ctx: M#Context, session: M#Session, t: ClassTag[M]) {
    val logger = org.slf4j.LoggerFactory.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-source-producer")

    def from(query: String, key: String, offset: Long = 0, maxPartitionSize: Long = 5000000l): M#Stream[M#Record] = {
      Storage[M].log(session, query, key, offset, maxPartitionSize, logger, ctx)
    }
  }
}