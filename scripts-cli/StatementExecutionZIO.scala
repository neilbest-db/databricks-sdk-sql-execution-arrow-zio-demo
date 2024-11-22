
import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

import io.delta.tables.DeltaTable

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  DataFrame,
  PublicArrowConverters => ArrowConverters }

import zio._
import zio.spark.experimental.ZIOSparkAppDefault
import zio.spark.parameter.localAllNodes
import zio.spark.rdd.RDD
import zio.spark.sql.{
  fromSpark, SparkSession}

import zio.stream.{
  ZStream, ZPipeline, ZSink}

import zio.logging.backend.SLF4J

import scala.collection.JavaConverters._

// import scribe.Logging


import databricks._
import warehouse._
import execution._


object StatementExecutionZIO extends ZIOSparkAppDefault {
    // with Logging {

  type ArrowBatch = Array[ Byte]

  scribe.Logger.minimumLevels(
    // "com.databricks.sdk" -> scribe.Level.Debug,   // show Databricks REST API calls
    "org.apache.hadoop" -> scribe.Level.Info,
    "org.apache.spark" -> scribe.Level.Error,        // make Spark be quiet!
    "org.apache.parquet" -> scribe.Level.Warn,
    "org.sparkproject.jetty" -> scribe.Level.Warn)

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val spark: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {

      ZIO.acquireRelease {

        val builder =
          SparkSession.builder
            .master( localAllNodes)
            .appName("arrow")
            .configs( Map(
              "spark.jars.packages" ->
                "io.delta:delta-core_2.12:2.4.0",
              "spark.sql.extensions"->
                "io.delta.sql.DeltaSparkSessionExtension",
              "spark.sql.catalog.spark_catalog" ->
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

        builder.getOrCreate

      } {

        ss =>
        ( ZIO.logInfo("Closing Spark Session ...")
          *> ss.close.tapError(
            _ => ZIO.logError("Failed to close the Spark Session."))
          .orDie)

      }

    }

  val query =
    """SELECT *
      |FROM audit
      |WHERE event_date = '2024-11-01'
      |LIMIT 10000000
      |;""".stripMargin

  val storageTarget = os.pwd / "delta/df"

  val untilRunning =
    ( Schedule.spaced( 3.seconds).upTo( 5.minutes)
      *> Schedule.recurUntil( ( s: sql.State) => sql.State.RUNNING.equals( s)))

  val untilSucceeded = 
    ( Schedule.spaced( 3.seconds).upTo( 15.minutes)
      *> Schedule.recurUntil( ( s: sql.StatementState) => s.equals( sql.StatementState.SUCCEEDED)))


  val successfulExecution: ZIO[ SqlExecutionService, Throwable, SqlStatement] =
    for {

    sqlExecutionService <- ZIO.service[ SqlExecutionService]

    _ <- ZIO.when( os.exists( storageTarget))(
      ZIO.log( s"Clearing storage target ${storageTarget}")
        *> ZIO.attempt( os.remove.all( storageTarget)))

    warehouseState <- sqlExecutionService.getWarehouseState.repeat( untilRunning)

    _ <- ZIO.unless(
      warehouseState.equals( sql.State.RUNNING))(
        ZIO.dieMessage(
          s"Starting warehouse timed out in state ${warehouseState}."))

    sqlExecution <-  sqlExecutionService.executeStatement( query)

    executionState <- sqlExecutionService.getState( sqlExecution).repeat( untilSucceeded)

    _ <- ZIO.unless(
      executionState.equals( sql.StatementState.SUCCEEDED))(
        ZIO.dieMessage(
          s"Statement execution timed out in state ${executionState}."))

  } yield sqlExecution.refresh


  val totalRecordsAppended: ZIO[ SqlExecutionService & SparkSession, Throwable, Unit] =
    for {

    spark <- ZIO.service[ SparkSession]
      statement <- successfulExecution

    _  <- fromSpark { spark =>
      DeltaTable
        .create( spark)
        .addColumns( result.schema)
        .location( storageTarget.toString)
        .execute
    }

      n <- statement.results.run(
        logProgress( statement.totalChunkCount)
          >>> httpStreams
          >>> arrowBatches
          >>> rdd // dataFrames( result.schema)
          >>> appendToDelta( statement.schema)
          >>> countRows)

    _ <- ZIO.log( s"Total records appended: $n")

  } yield ()

   def logProgress( n: Long) = // : ZPipeline[ . . . ] =
     ZPipeline.tap[ Any, Throwable, sql.ResultData](
       result => ZIO.log( s"Processing chunk ${result.getChunkIndex} of $n"))


  val httpStreams: ZPipeline[ Any, Throwable, sql.ResultData, Task[geny.Readable]] =
    ZPipeline.map(( result: sql.ResultData) => result.getExternalLinks.asScala)
      .flattenIterables[ sql.ExternalLink]
      .map( link => ZIO.attempt( requests.get.stream( link.getExternalLink)))
    // TODO: throws requests.RequestFailedException ("403 . . . expired")


  val arrowBatches: ZPipeline[ Any, Throwable, Task[ geny.Readable], Task[ Iterator[ ArrowBatch]]] =
    ZPipeline.mapZIOParUnordered(12) {
      task => for {
        stream <- task
        batches = ArrowConverters.getBatchesFromStream(
          stream.readBytesThrough(
            (is: java.io.InputStream)
              => java.nio.channels.Channels.newChannel( is)))
      } yield ZIO.attempt( batches)
    }

  val rdd: ZPipeline[ SparkSession, Throwable, Task[ Iterator[ ArrowBatch]], RDD[ ArrowBatch]] =
    ZPipeline.mapZIOParUnordered(12) {
      task => for {
        spark <- ZIO.service[ SparkSession]
        batches <- task
        rdd = spark.sparkContext.makeRDD(
          // batches.iterator,  // Spark > 3.3.2 (?)
          batches.toSeq)
      } yield rdd
    }
  

  def appendToDelta( schema: StructType): ZPipeline[ SparkSession, Throwable, RDD[ ArrowBatch], Long] =
    ZPipeline.mapZIOParUnordered(12) {
      rdd => {
        fromSpark { spark =>
          val df = ArrowConverters.toDataFrame(
            rdd.underlying.toJavaRDD,
            schema.json,
            spark )
          df.write.format("delta").mode("append")
            .save( storageTarget.toString)
          df.count
        }
      }
    }


  val countRows: ZSink[ Any, Throwable, Long, Nothing, Long] =
    ZSink.sum[Long]



  def run =
    ZIO.logLevel( LogLevel.Info) {
      totalRecordsAppended
        .provide(
          SqlExecutionService.layer,
          spark //,
                // ZLayer.Debug.tree
        )
    }


}









