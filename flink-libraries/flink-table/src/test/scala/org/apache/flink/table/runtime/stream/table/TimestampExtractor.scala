package org.apache.flink.table.runtime.stream.table

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class TimestampExtractor extends AssignerWithPunctuatedWatermarks[Tuple3[Long, Long, String]] {
  override def extractTimestamp(element: Tuple3[Long, Long, String], previousTimestamp: Long): Long = element._1

  override def checkAndGetNextWatermark(lastElement: Tuple3[Long, Long, String], extractedTimestamp: Long) = new Watermark(lastElement._1 - 1)
}
