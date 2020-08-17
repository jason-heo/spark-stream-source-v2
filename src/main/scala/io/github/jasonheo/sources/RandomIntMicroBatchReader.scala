package io.github.jasonheo.sources

import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.util.Random

class RandomIntMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends MicroBatchReader with Logging {
  private val numPartitions:Int = options.get("numPartitions").get.toInt

  // data source에서 message를 식별하는 용도로 사용한다
  // 예제에서는 0부터 1씩 증가하는 번호로 사용하였으나, 각자의 목적에 맞게 사용하면 된다
  // kafka의 경우 sequential한 숫자를 사용하므로 LongOffset을 사용하기 알맞다
  private var start: LongOffset = _
  private var end: LongOffset = _

  private var numNewRows: Long = _

  /**
    * @param _start 직전 micro batch read에서의 end offset
    * @param _end Optional.empty인 경우: `_start` 이후부터 신규로 도착한 메시지의 offset을 설정
    *             empty가 아닌 경우 `_end` 그대로 사용
    * 의문점: micro batch마다 `setOffsetRange()`가 두 번 호출되는데, `_end`가 처음에는 Optional.empty였다가,
    *       두 번째 호출에서는 `getEndOffset()`의 return value로 전달된다
    *       왜 이런 식으로 두 번 호출되는지 모르겠음. structured streaming에는 back-pressure도 없는데
    */
  override def setOffsetRange(_start: Optional[Offset], _end: Optional[Offset]): Unit = {
    logInfo(s"setOffsetRange(start_='${_start}', end_='${_end}') called")

    this.start = _start.orElse(LongOffset(0L)).asInstanceOf[LongOffset]

    // `end`에는 argument로 전달된 `_end` + (data source에 신규로 인입된 메시지 건수)를 저장한다
    this.end = if (_end.isPresent) {
      _end.get.asInstanceOf[LongOffset]
    }
    else {
      LongOffset(start.offset + getNumNewMsg())
    }
  }

  /**
    * 예제 프로그램에서는 0~2개의 로그가 신규로 인입되었다고 가정한다
    */
  private def getNumNewMsg(): Long = {
    val randomGenerator: Random = new scala.util.Random

    numNewRows = randomGenerator.nextInt(3).asInstanceOf[Long]

    logInfo(s"in getNumNewMsg(), numNewsRows='${numNewRows}'")
    numNewRows
  }

  override def getStartOffset: Offset = {
    logInfo(s"getStartOffset() called")

    logInfo(s"getStartOffset() returns '${start}'")

    start
  }

  override def getEndOffset: Offset = {
    logInfo(s"getEndOffset() called")

    logInfo(s"getEndOffset() returns '${end}'")

    end
  }

  override def deserializeOffset(json: String): Offset = {
    logInfo(s"deserializeOffset() returns '${LongOffset(json.toLong)}'")

    LongOffset(json.toLong)
  }

  /**
    * Spark Streaming에서 `end`까지 micro batch가 완료된 것을 의미한다
    * 활용처: data source에 `end`까지 처리 것을 알리는 용도로 사용하거나 내부에 저장 중인 자료를 삭제하면 된다
    */
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
        new RandomIntBatchInputPartition(partitionId, start.offset, numNewRows): InputPartition[InternalRow]
    }.toList
  }

  override def stop(): Unit = {
    logInfo(s"stop(end='${end}') called")
  }
}

class RandomIntBatchInputPartition(partitionId: Int, startOffset: Long, numNewRows: Long) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new RandomIntMicroBatchInputPartitionReader(partitionId, startOffset, numNewRows)
}

class RandomIntMicroBatchInputPartitionReader(partitionId: Int,
                                              startOffset: Long,
                                              numNewRows: Long) extends InputPartitionReader[InternalRow] with Logging {
  var cnt: Long = 0
  val randomIntGenerator: Random = new scala.util.Random
  var randomInt: Int = _

  override def next(): Boolean = {
    logInfo(s"[partition-${partitionId} next() called")

    // next()가 false를 return한 경우 micro batch read가 종료된다
    // numNewRows보다 개수를 더 적거나 많이 return하더라도 문제없다
    if (cnt < numNewRows) {
      randomInt = randomIntGenerator.nextInt(10)
      cnt += 1

      true
    }
    else {
      false
    }
  }

  // API를 보면 다음번 next()가 호출되기 전까지 get()은 항상 동일한 Row를 return해야한다고 한다
  // 그래서 `randomInt`를 next() 함수 내에서 생성했다
  override def get(): InternalRow = {
    logInfo(s"[partition-${partitionId} get() called")
    InternalRow(partitionId, startOffset + cnt, randomInt)
  }

  override def close(): Unit = {
    logInfo(s"[partition-${partitionId} close() called")
  }
}
