package org.apache.spark.sql

// import $ivy.`org.apache.spark::spark-sql:3.4.0`

// import org.apache.spark.sql.{
//   DataFrame, Dataset, SparkSession
// }

// import org.apache.spark.sql.execution.arrow.ArrowConverters.toDataFrame
import org.apache.spark.sql.execution.arrow.{
  ArrowConverters => PrivateArrowConverters
}

// import org.apache.spark.sql.util.ArrowUtils


object PublicArrowConverters {

  def toDataFrame(
    arrowBatches: Iterator[Array[Byte]],
    schemaString: String,
    session: SparkSession): DataFrame = {

    PrivateArrowConverters.toDataFrame(
      arrowBatches,
      schemaString,
      session)

  }

  val getBatchesFromStream = PrivateArrowConverters.getBatchesFromStream _

}
