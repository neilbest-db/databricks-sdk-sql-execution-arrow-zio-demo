

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
  PublicArrowConverters => ArrowConverters }

import zio._
import zio.spark.experimental.ZIOSparkAppDefault
import zio.spark.parameter.localAllNodes
import zio.spark.sql.{
  fromSpark, SparkSession}

// import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.backend.SLF4J
import zio.logging.{ LogFormat, LogFilter }

import scala.collection.JavaConverters._

// import scribe.Logging


import databricks._
import warehouse._
import execution._



object SqlExecutionApp extends ZIOSparkAppDefault { // with Logging {


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
  scribe.Logger("org") // .root
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some( scribe.Level.Warn))
    .replace()

  scribe.Logger("org.apache.spark")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some( scribe.Level.Warn))
    .replace()

  scribe.Logger("org.sparkproject.jetty")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some( scribe.Level.Error))
    .replace()

  scribe.Logger("org.apache.spark.storage.BlockManagerInfo")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some(scribe.Level.Error))
    .replace()
   */

  // no effect under Scribe
  val logFormat =
     SLF4J.logFormatDefault.filter(
      LogFilter.logLevelByName(
        LogLevel.Warning,
        "org.apache.spark" -> LogLevel.Warning,
        "com.databricks.sdk" -> LogLevel.Debug,      // no effect
        "org.sparkproject.jetty" -> LogLevel.Error))

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j // ( logFormat)

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

        /*
         *   no effect

        val disableSparkLogging: UIO[Unit] =
          ZIO.succeed(
          // Logger.getLogger("org.apache.spark").setLevel(Level.OFF))
          Logger.getRootLogger().setLevel(Level.WARN))


        for {
          _ <- ZIO.logInfo("Opening Spark Session...")
          spark <- builder.getOrCreate
          _ <- ZIO.succeed( spark.sparkContext.underlying.setLogLevel( "WARN"))
          _ <- disableSparkLogging
        } yield spark

         */

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

  val app = for {

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

    succeededExecution = sqlExecution.refresh

    links <- ZIO.loop(0)(
      _ < 20, _ + 1)(
      // _ < succeededExecution.chunkCount, _ + 1)(
      n => ZIO.attempt(
        succeededExecution.chunk( n).getExternalLinks.asScala))

    // TODO: throws requests.RequestFailedException ("403 . . . expired")
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

      val javaRDD: JavaRDD[ Array[ Byte]] =
        spark.sparkContext.parallelize( batches).toJavaRDD

      ArrowConverters.toDataFrame(
        // batches.iterator,  // Spark > 3.3.2 (?)
        javaRDD,
        succeededExecution.schema.json,
        spark)
    }

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


  /*
   * TODO: filter Spark logs <= INFO at runtime
   *
   * . . .  so that this ZIO app can emit at INFO level.
   *
   * `SPARK_CONF_DIR` had no effect; maybe it's only for `spark-shell`
   * & `spark-submit`?
   *
   */



  def run = ZIO.logLevel( LogLevel.Warning) {
    app
    .provide(
      SqlExecutionService.layer,
      spark //,
      // ZLayer.Debug.tree
    )
  }

  /*
   * what happens at runtime that Scribe does not have/use the loggers
   * defined here (at compile-time?)
   */

  val loggerNames = scribe.Logger.loggersByName.keys

  scribe.warn( s"Logger names: ${loggerNames}")



}









