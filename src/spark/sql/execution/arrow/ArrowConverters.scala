// package spark.sql.execution.arrow


// import org.apache.spark.sql.PublicArrowConverters


// object ArrowConverters {

//   import org.apache.spark.sql.{
//     DataFrame, Dataset, SparkSession
//   }

//   def toDataFrame(
//     arrowBatches: Iterator[Array[Byte]],
//     schemaString: String,
//     session: SparkSession
//   ): DataFrame =
//     PublicArrowConverters.toDataFrame(
//       arrowBatches,
//       schemaString,
//       session)

//   val getBatchesFromStream = PublicArrowConverters.getBatchesFromStream
// }

