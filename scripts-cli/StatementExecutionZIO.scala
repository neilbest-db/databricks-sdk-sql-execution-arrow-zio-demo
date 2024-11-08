// import $cp.^.arrow
// //> using jar ../arrow.jar

// import spark.sql.execution.arrow.ArrowConverters
/*
 import org.apache.spark.sql.{
  PublicArrowConverters => ArrowConverters
}
 */

// import $ivy.`org.apache.spark::spark-sql:3.4.0`
// //> using dep org.apache.spark::spark-sql:3.4.0

// import $ivy.`org.apache.spark::spark-core:3.4.0`
// import $ivy.`io.delta:delta-core_2.12:2.4.0`

// import $ivy.`com.databricks:databricks-sdk-java:0.34.0`
// //> using dep com.databricks:databricks-sdk-java:0.34.0  

// import $ivy.`dev.zio::zio:2.1.11`
// //> using dep dev.zio::zio:2.1.11


import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import zio._
// import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.backend.SLF4J
// import scala.collection.JavaConverters._

// import java.time.Duration


// import $file.databricks, databricks._
// import $file.warehouse, warehouse._
// //> using files databricks.sc warehouse.sc

import databricks._
import warehouse._
import execution._





object SqlExecutionApp extends ZIOAppDefault {

  /*
  scribe.Logger("com.databricks.sdk")
    .clearHandlers()
    .clearModifiers()
    .withHandler( minimumLevel = Some(scribe.Level.Debug))
    .replace()
   */

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j



  // def main() = {

  /*
  Unsafe.unsafe {
    implicit unsafe => {
   */
      val query =
        """SELECT *
          |FROM audit
          |WHERE event_date = '2024-11-01'
          |;""".stripMargin


  val untilRunning =
    ( Schedule.spaced( 3.seconds).upTo( 3.minutes)
      *> Schedule.recurUntil(( s: sql.State) => s.equals( sql.State.RUNNING)))

  val untilSucceeded = 
    ( Schedule.spaced( 20.seconds).upTo( 10.minutes)
      *> Schedule.recurUntil(( s: sql.StatementState) => s.equals( sql.StatementState.SUCCEEDED)))

  val app = for {

    // sqlWarehouse <- ZIO.service[ SqlWarehouse]

    sqlExecution <- ZIO.service[ SqlExecutionService]

    _ <- ZIO.log( "Starting warehouse . . .")
    
    // state <- {
    //   sqlExecution.getWarehouseState
    //     .repeatUntil( s => s.equals( sql.State.RUNNING))
    //     .schedule( Schedule.spaced( 3.seconds))
    //     .timeoutFail( "Warehouse took too long to start.")(3.minutes)
    // }

    _ <- {
      ZIO.whenCaseZIO(
        sqlExecution.getWarehouseState
          .repeat( untilRunning)) {
        case sql.State.RUNNING => ZIO.log( "Warehouse state is RUNNING.")
        case s => ZIO.dieMessage( s"Warehouse timed out in state ${s}.")
      }
    }

    statement <- sqlExecution.executeStatement( query)

    _ <- ZIO.log( "Executing statement . . .")


    _ <- {
      ZIO.whenCaseZIO(
        sqlExecution.getState( statement)
          .repeat( untilSucceeded)) {
        case sql.StatementState.SUCCEEDED => ZIO.log( "Statement execution SUCCEEDED.")
        case s => ZIO.dieMessage( s"Statement execution timed out in state ${s}.")
      }
    }

    result <- ZIO.attempt( statement.refresh.result)

    _ <- Console.printLine( s"Result: ${result}")


  /*
    val data: Iterator[ Iterator[ Array[ Byte]]] = for {
        link <- links
        stream <- requests.get.stream( link.getExternalLink())
        data <- ArrowConverters.getBatchesFromStream(
          stream.readBytesThrough(
            (is: java.io.InputStream)
              => java.nio.channels.Channels.newChannel( is)))
       } yield data
   */


  } yield ()


  // Runtime.default.unsafe.run(

  def run = app
    .provide(
      // WorkspaceClient.layer( "e2-demo-west-ws"),
      // SqlWarehouse.layer,
      SqlExecutionService.layer,
      ZLayer.Debug.tree)

}


/*


// val stream : geny.Readable =
//   requests.get.stream( gs().getResult.getExternalLinks.asScala.head.getExternalLink)

// println( gs)

lazy val streams =
  gs().getResult().getExternalLinks().iterator.asScala.map {
    el: sql.ExternalLink => requests.get.stream( el.getExternalLink())
  }

// throws requests.RequestFailedException ("403 . . . expired")


// val iab: Iterator[ Array[ Byte]] =
//   ArrowConverters.getBatchesFromStream(
//     stream.readBytesThrough(
//       (is: java.io.InputStream)
//         => java.nio.channels.Channels.newChannel( is)))

lazy val iabs: Iterator[Iterator[Array[Byte]]] =
  streams.map { stream =>
    ArrowConverters.getBatchesFromStream(
      stream.readBytesThrough(
        (is: java.io.InputStream)
          => java.nio.channels.Channels.newChannel( is)))
  }

lazy val schema =
  org.apache.spark.sql.types.StructType
    .fromDDL(
      gs().getManifest.getSchema.getColumns.asScala
        .map( ci => s"${ci.getName} ${ci.getTypeText}")
        .mkString( ",\n"))
    .json

lazy val spark = SparkSession.builder
  .master("local")
  .appName("arrow")
  .config( Map(
    "spark.jars.packages" ->
      "io.delta:delta-core_2.12:2.2.0",
    "spark.sql.extensions"->
      "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" ->
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
  .getOrCreate()

lazy val df = ArrowConverters.toDataFrame( iabs.next, schema, spark)

/*
@main
def main() = {

  Unsafe.unsafe { implicit unsafe =>

    // val url = URL.decode( link).toOption.get


    // val runtime = Runtime.default


    val logic = for {
      databricksConfig <- ZIO.service[ DatabricksConfig]
      workspaceClient <- ZIO.service[ WorkspaceClient]

      link <- ZIO.succeed( getLink())
      // url = URL.decode( link).toOption.get
      // client <- ZIO.service[Client]
      // res <- client.url( URL.decode( link).toOption.get).batched( Request.get("/"))
      // data <- res.body.asString
      // client <- ZIO.service[Client]
      _ <- Console.printLine( link)
    } yield ()

    val good =
      Client
        .batched(
          Request.get( getLink()))
        .flatMap(
          _.body.asString)

    Runtime.default.unsafe.run( good.provide( Client.default))

  }


}
 */


   /*
        """SELECT *
        |FROM audit
        |WHERE event_date = '2024-09-01'
        |;""".stripMargin)
     */

 */
