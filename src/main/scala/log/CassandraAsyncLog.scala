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

package log

import java.lang.{ Long ⇒ JLong }

import akka.stream.scaladsl.Source
import akka.stream.stage.{ AsyncCallback, GraphStage, GraphStageLogic, OutHandler }
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.datastax.driver.core.{ ResultSet, Row, Session, SimpleStatement }
import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }
import join.cassandra.CassandraSource

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

class CassandraAsyncLog(session: CassandraSource#Session, cassandraPageSize: Int = 512, query: String,
                        key: String, offset: Long, maxPartitionSize: Long) extends GraphStage[SourceShape[Row]] {

  val out: Outlet[Row] = Outlet("CassandraAsyncSource.out")

  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var requireMore = false
      var sequenceNr = offset
      var partitionIter = Option.empty[ResultSet]
      var onMessage: AsyncCallback[Try[ResultSet]] = _

      private def navigatePartition(n: Long, max: Long) = n / max

      override def preStart(): Unit = {
        implicit val ec = materializer.executionContext
        onMessage = getAsyncCallback[Try[ResultSet]](onFinish)
        guavaToScala(session.executeAsync(new SimpleStatement(query, key,
          navigatePartition(sequenceNr, maxPartitionSize): JLong, offset: JLong)
          .setFetchSize(cassandraPageSize))).onComplete(onMessage.invoke)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          implicit val ec = materializer.executionContext
          partitionIter match {
            case Some(iter) if iter.getAvailableWithoutFetching > 0 ⇒
              sequenceNr += 1
              push(out, iter.one)
            case Some(iter) if iter.isExhausted ⇒
              guavaToScala(session.executeAsync(new SimpleStatement(query, key,
                navigatePartition(sequenceNr, maxPartitionSize): JLong, sequenceNr: JLong)
                .setFetchSize(cassandraPageSize))).onComplete(onMessage.invoke)

            case Some(iter) ⇒ guavaToScala(iter.fetchMoreResults).onComplete(onMessage.invoke)
            case None       ⇒ ()
          }
        }
      })

      private def onFinish(rsOrFailure: Try[ResultSet]): Unit = rsOrFailure match {
        case Success(iter) ⇒
          partitionIter = Some(iter)
          if (iter.getAvailableWithoutFetching > 0) {
            if (isAvailable(out)) {
              sequenceNr += 1
              push(out, iter.one())
            }
          } else completeStage()

        case Failure(failure) ⇒ failStage(failure)
      }
    }

  private def guavaToScala[A](guavaFut: ListenableFuture[A]): Future[A] = {
    val p = Promise[A]()
    val callback = new FutureCallback[A] {
      override def onSuccess(a: A): Unit = p.success(a)
      override def onFailure(err: Throwable): Unit = p.failure(err)
    }
    Futures.addCallback(guavaFut, callback)
    p.future
  }
}

object CassandraAsyncLog {
  def apply(session: Session, cassandraPageSize: Int, cassandraQuery: String,
            key: String, offset: Long, maxPartitionSize: Long) =
    Source.fromGraph(new CassandraAsyncLog(session, cassandraPageSize, cassandraQuery, key, offset, maxPartitionSize))
}
