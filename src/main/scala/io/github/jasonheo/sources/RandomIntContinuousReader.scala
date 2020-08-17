package io.github.jasonheo.sources

import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{ContinuousInputPartition, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.util.Random

class RandomIntContinuousReader (options: DataSourceOptions) extends ContinuousReader with Logging {
  private val numPartitions:Int = options.get("numPartitions").get.toInt

  private var startOffset: LongOffset = _

  /**
    * ContinuousReader는 offset 관리 부분이 MicroBatchReader보다 더 어렵다
    * Offset 관련된 코드는 RateStreamProvider의 내용을 복사해온 내용이 있음
    */
  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    logInfo(s"mergeOffsets(offsets='${offsets}') called")

    val partitionOffsetTuple: Array[(Int, Long)] = offsets.map {
      case RandomIntContinuousPartitionOffset(partitionId, docSeq) =>
        (partitionId, docSeq)
    }

    val partitionOffsetMap: RandomIntContinuousOffset = RandomIntContinuousOffset(partitionOffsetTuple.toMap)

    logInfo(s"mergeOffsets() returns ${partitionOffsetMap}")

    partitionOffsetMap
  }

  override def deserializeOffset(json: String): Offset = {
    logInfo(s"deserializeOffset(json='${json}') called")
    implicit val defaultFormats: DefaultFormats = DefaultFormats

    val offset: RandomIntContinuousOffset = RandomIntContinuousOffset(Serialization.read[Map[Int, Long]](json))

    logInfo(s"deserializeOffset() returns ${offset}")

    offset
  }

  override def setStartOffset(_start: Optional[Offset]): Unit = {
    logInfo(s"setStartOffset(_start='${_start}') called")

    startOffset = _start.orElse(LongOffset(0L)).asInstanceOf[LongOffset]
  }

  override def getStartOffset: Offset = {
    logInfo(s"getStartOffset() called")

    logInfo(s"getStartOffset() returns ${startOffset}")

    startOffset
  }

  override def commit(end: Offset): Unit = {
    logInfo(s"commit(end='${end}') called")
  }

  override def readSchema(): StructType = {
    StructType(
      Array(
        StructField("partition_id", IntegerType, false),
        StructField("offset", LongType, false),
        StructField("random_int", IntegerType, false)
      )
    )
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    logInfo(s"planInputPartitions() called")

    Range(0, numPartitions).map {
      partitionId: Int =>
        // 편의를 위해, 모든 partition에서 numNewRows 만큼의 row를 return하도록 한다
        new RandomIntContinuousInputPartition(partitionId, startOffset.offset): InputPartition[InternalRow]
    }.toList
  }

  override def stop(): Unit = {
    logInfo(s"stop() called")
  }
}

class RandomIntContinuousInputPartition(partitionId: Int, startOffset: Long) extends ContinuousInputPartition[InternalRow] {
  override def createContinuousReader(offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    new RandomIntContinuousInputPartitionReader(partitionId, startOffset)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new RandomIntContinuousInputPartitionReader(partitionId, startOffset)
  }
}

class RandomIntContinuousInputPartitionReader(partitionId: Int, startOffset: Long)
    extends ContinuousInputPartitionReader[InternalRow] with Logging {

  val randomIntGenerator: Random = new scala.util.Random
  var randomInt: Int = _
  var cnt: Long = 0

  override def getOffset(): PartitionOffset = {
    logInfo(s"[partition-${partitionId}] getOffset() called")

    val offset = RandomIntContinuousPartitionOffset(partitionId, startOffset + cnt)

    logInfo(s"[partition-${partitionId}] getOffset() returns '${offset}'")

    offset
  }

  // next가 false를 return하면 Continuous Stream이 종료된다
  override def next(): Boolean = {
    logInfo(s"[partition-${partitionId}] next() called")

    // 예제 프로그램에서는 의도적으로 메시지를 늦게 생성한다
    // 0.8초 ~ 1.3초 사이에 1개의 레코드씩 생성한다
    // 너무 빠르게 생성되는 경우 로그 메시지가 많아서 분석이 어렵다
    Thread.sleep(800 + randomIntGenerator.nextInt(500))

    try {
      cnt += 1
      randomInt = randomIntGenerator.nextInt(10)

      true
    }
    catch {
      case e: InterruptedException =>
        // 사용자가 stream query를 종료한 경우
        false
    }
  }

  override def get(): InternalRow = {
    logInfo(s"[partition-${partitionId}] get() called")

    InternalRow(partitionId, startOffset + cnt, randomInt)
  }

  override def close(): Unit = {
    logInfo(s"[partition-${partitionId} close() called")
  }
}

case class RandomIntContinuousPartitionOffset(partitionId: Int, offset: Long) extends PartitionOffset

case class RandomIntContinuousOffset(partitionOffsetMap: Map[Int, Long]) extends Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  override val json = Serialization.write(partitionOffsetMap)
}
