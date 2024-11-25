
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
import zio.spark.sql.{ fromSpark, SparkSession}
import zio.stream.{ ZStream, ZPipeline, ZSink}
import zio.logging.backend.SLF4J

import java.io.InputStream
import java.nio.channels.Channels.newChannel
import scala.collection.JavaConverters._

// import scribe.Logging

import databricks._
import warehouse._
import statement.{ index, SqlStatement}
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
            .appName( "arrow")
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
      |LIMIT 20000000
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

      _ <- ZIO.unless( warehouseState.equals( sql.State.RUNNING))(
        ZIO.dieMessage( s"Starting warehouse timed out in state ${warehouseState}."))

      sqlExecution <-  sqlExecutionService.executeStatement( query)

      executionState <- sqlExecutionService.getState( sqlExecution).repeat( untilSucceeded)

      _ <- ZIO.unless( executionState.equals( sql.StatementState.SUCCEEDED))(
        ZIO.dieMessage( s"Statement execution timed out in state ${executionState}."))

  } yield sqlExecution.refresh


  val totalRecordsAppended: ZIO[ SqlExecutionService & SparkSession, Throwable, Unit] =
    for {

      statement <- successfulExecution

      spark <- ZIO.service[ SparkSession]

      _  <- fromSpark { spark =>
        DeltaTable
          .create( spark)
          .addColumns( statement.schema)
          .location( storageTarget.toString)
          .execute }

      n <- ZIO.logSpan( "pipeline") {
        statement.externalLinks.run(
            download
            >>> arrowBatches
            >>> rdd
            >>> appendToDelta( statement.schema)
            >>> countRows)
      } @@ index( "n")( statement.totalChunkCount)

      _ <- ZIO.log( s"Total records appended: $n")

    } yield ()

  val download: ZPipeline[ Any, Throwable, (sql.ExternalLink, Long), (geny.Readable, Long)] =
    ZPipeline.mapZIOPar(12) {
      case ( link, i) =>
        ZIO.log( "Downloading result chunk") @@ index( "i")( i) *>
        ZIO.attempt( ( requests.get.stream( link.getExternalLink), i))
    }

  // TODO: throws requests.TimeoutException ("Request to https://<link>?<REDACTME> timed out. (readTimeout: 10000, connectTimout: 10000)")
  // TODO: throws requests.RequestFailedException ("403 . . . expired")


  def getBatchesFromStream( stream: geny.Readable): Iterator[ ArrowBatch] =
    ArrowConverters.getBatchesFromStream(
      stream.readBytesThrough( (is: InputStream) => newChannel( is)))

  val arrowBatches: ZPipeline[ Any, Throwable, (geny.Readable, Long), (Iterator[ ArrowBatch], Long)] =
    ZPipeline.mapZIOParUnordered(12) {
      case (stream, i) =>
        ZIO.log( "Decoding Arrow batches") @@ index( "i")( i) *>
        ZIO.attempt( ( getBatchesFromStream( stream), i))
    }

  val rdd: ZPipeline[ SparkSession, Throwable, (Iterator[ ArrowBatch], Long), (RDD[ ArrowBatch], Long)] =
    ZPipeline.mapZIOParUnordered(12) {
      case (batches, i) => for {
        spark <- ZIO.service[ SparkSession]
        _ <- ZIO.log( "Creating Arrow batch RDD") @@ index( "i")( i)
        rdd = spark.sparkContext.makeRDD( batches.toSeq)  // batches.iterator),  // Spark > 3.3.2 (?)
      } yield (rdd, i)
    }

  def appendToDelta( schema: StructType): ZPipeline[ SparkSession, Throwable, (RDD[ ArrowBatch], Long), Long] =
    ZPipeline.mapZIOParUnordered(12) {
      case (rdd, i) =>  for {
        spark <- ZIO.service[ SparkSession]
        df <- fromSpark { spark => ArrowConverters.toDataFrame( rdd.underlying.toJavaRDD, schema.json, spark) }
        n_i = df.count
        _ <- ZIO.log( s"Writing $n_i records to Delta") @@ index( "i")( i)
        _ <- ZIO.attempt( df.write.format("delta").mode("append").save( storageTarget.toString))
        } yield n_i
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









