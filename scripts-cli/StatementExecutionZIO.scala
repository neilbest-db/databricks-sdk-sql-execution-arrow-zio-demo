
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

import io.delta.tables.DeltaTable

import zio._
import zio.spark.experimental.ZIOSparkAppDefault
import zio.spark.parameter.localAllNodes
import zio.spark.sql.{ fromSpark, SparkSession}
import zio.logging.backend.SLF4J

import databricks._
import execution._
import pipeline.ArrowPipeline._
import statement.SqlStatement
import warehouse._

object StatementExecutionZIO extends ZIOSparkAppDefault {

  logger.setup()

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val spark: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      ZIO.acquireRelease {
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
          .getOrCreate
      } {
        ss => ZIO.logInfo("Closing Spark Session ...") *>
        ss.close.tapError( _ => ZIO.logError("Failed to close the Spark Session.")).orDie
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
            >>> appendToDelta( statement.schema, storageTarget)
            >>> countRows)
      } @@ index( "n")( statement.totalChunkCount)

      _ <- ZIO.log( s"Total records appended: $n")

    } yield ()

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









