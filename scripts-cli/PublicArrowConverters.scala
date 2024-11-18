package org.apache.spark.sql

import org.apache.spark.api.java.JavaRDD

import org.apache.spark.sql.execution.arrow.{
  ArrowConverters => PrivateArrowConverters
}


object PublicArrowConverters {

  def toDataFrame(
    // arrowBatches: Iterator[ Array[ Byte]],  // Spark >= 3.4.0
    arrowBatches: JavaRDD[ Array[ Byte]],  // Spark <= 3.3.4
    schemaString: String,
    session: SparkSession): DataFrame = {

    PrivateArrowConverters.toDataFrame(
      arrowBatches,
      schemaString,
      session)

  }

  val getBatchesFromStream = PrivateArrowConverters.getBatchesFromStream _

}
