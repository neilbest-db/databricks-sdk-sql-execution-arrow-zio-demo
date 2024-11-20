

import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

// with Spark 3.1.1, 3.3.1(?) (Log4J)
import org.apache.log4j.{Level, Logger}

// with Spark 3.4.0 ? (SLF4J)
// import org.apache.log4j.Level
// import org.slf4j.{Logger,LoggerFactory}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  DataFrame,
  PublicArrowConverters => ArrowConverters }

import zio._
import zio.spark.experimental.ZIOSparkAppDefault
import zio.spark.parameter.localAllNodes
import zio.spark.sql.{
  fromSpark, SparkSession}

import zio.stream.{
  ZStream, ZPipeline, ZSink}
// import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.backend.SLF4J
import zio.logging.{ LogFormat, LogFilter }

import scala.collection.JavaConverters._

// import scribe.Logging


import databricks._
import warehouse._
import execution._


object SqlExecutionApp extends ZIOSparkAppDefault { // with Logging {

  type ArrowBatch = Array[ Byte]

  /*
   *  show Databricks REST API calls

  scribe.Logger("com.databricks.sdk")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some(scribe.Level.Debug))
    .replace()

   */

  /*
   *  make Spark be quiet!
   *  (no effect unless applied to the `root` logger)
   */

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some( scribe.Level.Warn))
    .replace()

  /*
   * TODO: filter Spark logs <= INFO at runtime
   *
   * . . .  so that this ZIO app can emit at INFO level.
   *
   * `SPARK_CONF_DIR` had no effect; maybe it's only for `spark-shell`
   * & `spark-submit`?
   *
   */

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

    // Runtime.removeDefaultLoggers >>> zio.logging.consoleLogger() >+> Slf4jBridge.init( logFilter)

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
      |;""".stripMargin

  val storageTarget = os.pwd / "delta/df"

  val untilRunning =
    ( Schedule.spaced( 3.seconds).upTo( 3.minutes)
      *> Schedule.recurUntil( ( s: sql.State) => sql.State.RUNNING.equals( s)))

  val untilSucceeded = 
    ( Schedule.spaced( 20.seconds).upTo( 10.minutes)
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


  val httpStreams: ZPipeline[ Any, Throwable, sql.ExternalLink, geny.Readable] =
    ZPipeline.map( link => requests.get.stream( link.getExternalLink))
  // TODO: throws requests.RequestFailedException ("403 . . . expired")

  val arrowBatches: ZPipeline[ Any, Throwable, geny.Readable, Iterator[ ArrowBatch]] =
    ZPipeline.map(
      (stream: geny.Readable) => ArrowConverters.getBatchesFromStream(
        stream.readBytesThrough(
          (is: java.io.InputStream) => java.nio.channels.Channels.newChannel( is))))


  def dataFrame( schema: StructType): ZPipeline[ SparkSession, Throwable, Iterator[ ArrowBatch], DataFrame] =
    ZPipeline.mapZIOParUnordered(4) {
      ( batches: Iterator[ ArrowBatch]) => fromSpark {
        spark => ArrowConverters.toDataFrame(
          // batches.iterator,  // Spark > 3.3.2 (?)
          spark.sparkContext.parallelize( batches.toSeq).toJavaRDD,
          schema.json,
          spark)
      }
    }
  

  val appendToDelta: ZPipeline[ SparkSession, Throwable, DataFrame, Long] =
    ZPipeline.map {
      ( df: DataFrame) => {
        df.write.format("delta")
          .mode("append")
          .save( storageTarget.toString)
        df.count
      }
    }

  val dataFrameCounts: ZSink[ Any, Throwable, Long, Nothing, Long] =
    ZSink.sum[Long]


  val totalRecordsAppended = for {

    result <- successfulExecution

    n <- result.links.run(
      httpStreams
        >>> arrowBatches
        >>> dataFrame( result.schema) //successfulExecution.tap( _.schema)
        >>> appendToDelta
        >>> dataFrameCounts)

    _ <- ZIO.log( s"Total records appended: $n")

  } yield ()

  def run =
    ZIO.logLevel( LogLevel.Warning) {
      totalRecordsAppended
        .provide(
          SqlExecutionService.layer,
          spark //,
                // ZLayer.Debug.tree
        )
    }


}









