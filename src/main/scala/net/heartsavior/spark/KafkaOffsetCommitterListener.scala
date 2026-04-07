/*
 * Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.heartsavior.spark

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.streaming.runtime.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.kafka010.KafkaSourceInspector
import org.apache.spark.sql.streaming.StreamingQueryListener


class KafkaOffsetCommitterListener extends StreamingQueryListener with Logging {
  import KafkaOffsetCommitterListener._

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val query = SparkSession.active.streams.get(event.progress.id)
    if (query != null) {
      val streamExecOpt: Option[StreamExecution] = query match {
        case wrapper: StreamingQueryWrapper => Option(wrapper.streamingQuery)
        case se: StreamExecution => Option(se)
        case _ =>
          logWarning(s"Unexpected type of streaming query: ${query.getClass}")
          None
      }

      streamExecOpt.foreach { streamExec =>
        val ex = streamExec.lastExecution
        if (ex != null) {
          // Build a `SparkDataStream -> source-progress index` map from
          // `streamExec.sources`. The order of that list matches the order of
          // `event.progress.sources`, but the executed plan only contains scans
          // for the sources that have new data in the current micro-batch, so
          // matching by leaf position is unsafe — we have to match by stream
          // identity instead. `StreamExecution.sources` is `protected` in Scala
          // (subclass-only), so we have to read it via reflection. The per-leaf
          // stream lookup itself is typed and lives in
          // KafkaSourceInspector.populateKafkaParams.
          val sourcesOpt =
            ReflectionHelper.reflectFieldWithContextClassloader[Seq[SparkDataStream]](
              streamExec, "sources")
          val streamToProgressIdx: Map[SparkDataStream, Int] =
            sourcesOpt.map(_.zipWithIndex.toMap).getOrElse(Map.empty)

          val inspector = new KafkaSourceInspector(ex.executedPlan)
          inspector.populateKafkaParams.foreach { case (streamOpt, params) =>
            params.get(CONFIG_KEY_GROUP_ID) match {
              case Some(groupId) =>
                val progressIdxOpt = streamOpt.flatMap(streamToProgressIdx.get)
                progressIdxOpt match {
                  case Some(idx) =>
                    val sourceProgress = event.progress.sources(idx)
                    val tpToOffsets = inspector.partitionOffsets(sourceProgress.endOffset)

                    val newParams = new scala.collection.mutable.HashMap[String, Object]
                    newParams ++= params
                    newParams += "group.id" -> groupId

                    val kafkaConsumer = new KafkaConsumer[String, String](newParams.asJava)
                    try {
                      val offsetsToCommit = tpToOffsets.map { case (tp, offset) =>
                        tp -> new OffsetAndMetadata(offset)
                      }
                      kafkaConsumer.commitSync(offsetsToCommit.asJava, Duration.ofSeconds(10))
                    } finally {
                      kafkaConsumer.close()
                    }

                  case None =>
                    logWarning("Could not match a Kafka plan leaf with group id " +
                      s"'$groupId' to any streaming source in query ${event.progress.id}; " +
                      "offsets will not be committed for this leaf.")
                }

              case None =>
            }
          }
        }
      }
    } else {
      logWarning(s"Cannot find query ${event.progress.id} from active spark session!")
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

object KafkaOffsetCommitterListener {
  private val CONFIG_KEY_GROUP_ID = "consumer.commit.groupid"
  val CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION: String = "kafka." + CONFIG_KEY_GROUP_ID
}
