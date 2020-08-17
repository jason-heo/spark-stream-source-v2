package io.github.jasonheo.sources

import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader}
import org.apache.spark.sql.types.StructType

class RandomIntStreamProvider extends DataSourceV2
  with MicroBatchReadSupport with ContinuousReadSupport with DataSourceRegister with Logging {

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    if (schema.isPresent) {
      throw new IllegalArgumentException("The random-int source does not support a user-specified schema.")
    }

    new RandomIntMicroBatchReader(options, checkpointLocation)
  }

  override def createContinuousReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): ContinuousReader = {
    new RandomIntContinuousReader(options)
  }

  override def shortName(): String = "random-int"
}

