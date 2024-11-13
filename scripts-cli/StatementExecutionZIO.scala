

import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  PublicArrowConverters => ArrowConverters }

import zio._
import zio.spark.experimental.ZIOSparkAppDefault
import zio.spark.parameter.localAllNodes
import zio.spark.sql.{
  fromSpark, SparkSession}

// import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.backend.SLF4J

import scala.jdk.CollectionConverters._

import databricks._
import warehouse._
import execution._


object SqlExecutionApp extends ZIOSparkAppDefault {

   // show Databricks REST API calls
   
  scribe.Logger("com.databricks.sdk")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some(scribe.Level.Debug))
    .replace()


  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

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

  val app = for {

    sqlExecutionService <- ZIO.service[ SqlExecutionService]

    _ <- ZIO.when( os.exists( storageTarget))(
      ZIO.log( s"Clearing storage target ${storageTarget}")
        *> ZIO.attempt( os.remove.all( storageTarget)))

    // _ <- ZIO.log( "Starting warehouse . . .")

    warehouseState <- sqlExecutionService.getWarehouseState.repeat( untilRunning)

    _ <- ZIO.unless(
      warehouseState.equals( sql.State.RUNNING))(
        ZIO.dieMessage(
          s"Starting warehouse timed out in state ${warehouseState}."))

    sqlExecution <-  sqlExecutionService.executeStatement( query)

    // _ <- ZIO.log( "Executing statement . . .")


    executionState <- sqlExecutionService.getState( sqlExecution).repeat( untilSucceeded)

    _ <- ZIO.unless(
      executionState.equals( sql.StatementState.SUCCEEDED))(
        ZIO.dieMessage(
          s"Statement execution timed out in state ${executionState}."))

    succeededExecution = sqlExecution.refresh

    links <- ZIO.loop(0)( _ < 20, _ + 1)(
      // _ < executed.chunkCount, _ + 1)(
      n => ZIO.attempt( succeededExecution.chunk( n).getExternalLinks.asScala))

    // links <- ZIO.succeed( executed.links.map( _.getExternalLink))

    streams <- ZIO.attempt(
      links.flatten.map( _.getExternalLink).map( link => requests.get.stream( link)))

    batches <- ZIO.attempt(
      for {

        stream <- streams

        batch <- ArrowConverters.getBatchesFromStream(
          stream.readBytesThrough(
            (is: java.io.InputStream)
              => java.nio.channels.Channels.newChannel( is)))

      } yield batch)

    df <- fromSpark { spark =>
      ArrowConverters.toDataFrame(
        batches.iterator,
        succeededExecution.schema.json,
        spark)
    }

    // _ <- Console.printLine( s"Row count: ${df.count}")
    // _ <- ZIO.log( s"Row count: ${df.count}")

    _ <- fromSpark { spark =>
      df.write.format("delta")
        .mode("append")
        .save( storageTarget.toString)
    }

    records <- fromSpark { spark =>
      spark.read.format( "delta")
        .load( storageTarget.toString)
        .count
    }

    _ <- ZIO.log( s"Row count: ${records}")

  } yield ()

  private val spark: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      val disableSparkLogging: UIO[Unit] =
        ZIO.succeed(
          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR))

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

      val build: ZIO[Scope, Throwable, SparkSession] =
        builder.getOrCreate.withFinalizer { ss =>
          ZIO.logInfo("Closing Spark Session ...") *>
            ss.close.tapError(_ => ZIO.logError("Failed to close the Spark Session.")).orDie
        }

      ZIO.logInfo("Opening Spark Session...") *> disableSparkLogging *> build
    }

  scribe.Logger("org.apache.spark")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some(scribe.Level.Error))
    .replace()

  def run = app
    .provide(
      SqlExecutionService.layer,
      spark //,
      // ZLayer.Debug.tree
    )

}




// throws requests.RequestFailedException ("403 . . . expired")




// lazy val df = ArrowConverters.toDataFrame( iabs.next, schema, spark)
