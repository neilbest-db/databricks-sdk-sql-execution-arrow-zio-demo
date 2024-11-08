// import $cp.^.arrow
// import spark.sql.execution.arrow.ArrowConverters
/*
 import org.apache.spark.sql.{
  PublicArrowConverters => ArrowConverters
}
 */

import $ivy.`org.apache.spark::spark-sql:3.4.0`
// import $ivy.`org.apache.spark::spark-core:3.4.0`
// import $ivy.`io.delta:delta-core_2.12:2.4.0`
import $ivy.`com.databricks:databricks-sdk-java:0.34.0`
import $ivy.`dev.zio::zio:2.1.11`
// import $ivy.`dev.zio::zio-http:3.0.1`

// import $ivy.`io.netty:netty-buffer:4.1.110.Final`


import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import zio._
// import zio.http._

// import scala.collection.JavaConverters._

// import java.time.Duration



// import $file.databricks, databricks._
// import $file.warehouse, warehouse._


case class SqlExecutionService( sqlWarehouse: SqlWarehouse) {


  def isWarehouseRunning: ZIO[ Any, Throwable, Boolean] =
    ZIO.attempt( sqlWarehouse.refresh)
      .map( _.state == sql.State.RUNNING)

  def executeStatement( statement: String): ZIO[ Any, Throwable, SqlStatement] = {
    // val pretty = statement.linesWithSeparators.toSeq.mkString("\t")
    // ZIO.debug( s"Submitting statement:\n\t${pretty}")
    ZIO.attempt( sqlWarehouse.execute( statement))
  }

  def getState( sqlStatement: SqlStatement): ZIO[ Any, Throwable, sql.StatementState] = {
    ZIO.attempt( sqlStatement.refresh.response.getStatus.getState)
  }

}

object SqlExecutionService {

  def isWarehouseRunning: ZIO[ SqlExecutionService, Throwable, Boolean] =
    ZIO.serviceWithZIO( _.isWarehouseRunning)

  def executeStatement( statement: String): ZIO[ SqlExecutionService, Throwable, SqlStatement] = {
    ZIO.serviceWithZIO( _.executeStatement( statement))
  }

  def getState( statement: SqlStatement): ZIO[ SqlExecutionService, Throwable, sql.StatementState] = {
    ZIO.serviceWithZIO( _.getState( statement))
  }

  val layer: ZLayer[ SqlWarehouse, Nothing, SqlExecutionService] =
    ZLayer {
      for {
        sqlWarehouse <- ZIO.service[ SqlWarehouse]
      } yield SqlExecutionService( sqlWarehouse)
    }

}




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

// object StatementExecutionApp extends ZIOAppDefault {

@main
def main() = {

  Unsafe.unsafe {
    implicit unsafe => {

      val query =
        """SELECT *
          |FROM audit
          |WHERE event_date = '2024-11-01'
          |;""".stripMargin


      val app = for {

        sqlWarehouse <- ZIO.service[ SqlWarehouse]

        sqlExecution <- ZIO.service[ SqlExecutionService]

        _ <- {
          ZIO.attempt( sqlExecution.isWarehouseRunning)
            .repeatUntilEquals( true)
            .schedule( Schedule.spaced( 3.seconds))
            .timeoutFail(
              ZIO.fail( "Warehouse took too long to start.")
            )(5.minutes)
        }

        statement <- ZIO.attempt( sqlExecution.executeStatement( query))

        _ <- {
          statement.map( s => sqlExecution.getState( s))
            .repeatUntilEquals( sql.StatementState.SUCCEEDED)
            .schedule( Schedule.linear( 3.seconds))
            .timeoutFail(
              ZIO.fail( "Query took too long to execute.")
            )(5.minutes)
        }

        result <- statement.map( s => s.refresh.result)

      } yield result

      Runtime.default.unsafe.run(
        app.provide(
          // WorkspaceClient.layer( "e2-demo-west-ws"),
          SqlWarehouse.layer(
            "Shared Endpoint",
            WorkspaceClient(
              DatabricksConfig(
                "e2-demo-west-ws")))))

    }
  }
}



/*

val w = WorkspaceClient( DatabricksConfig( "e2-demo-west-ws"))

// val sqlWh = () => SqlWarehouse( "Shared Endpoint", w).start
// val sqlWh = SqlWarehouse( "Shared Endpoint", w).start
val sqlWh = SqlWarehouse( "Shared Endpoint").start

// val sqlWhState = () => sqlWh.refresh.it.getState()

while ( sqlWh.state != sql.State.RUNNING) {
  println( s"Waiting for SQL Warehouse '${sqlWh.it.getName()}' to start . . .")
  Thread.sleep( 3000)
}


println( s"${sqlWh.it.getName()} state is: ${sqlWhState()}")

val esr = new sql.ExecuteStatementRequest()
    .setWarehouseId( sqlWh.id)  //.it.getId())
    .setCatalog( "system")
    .setSchema( "access")
    .setDisposition( sql.Disposition.EXTERNAL_LINKS)
    .setFormat( sql.Format.ARROW_STREAM)
    .setStatement(
      """SELECT *
        |FROM audit
        |WHERE event_date = '2024-09-01'
        |;""".stripMargin)

val esrResponse = w.it.statementExecution().executeStatement( esr)



println( s"Statement ID is ${esrResponse.getStatementId}")

// Thread.sleep( 5000)

val gs = () =>
  w.it.statementExecution().getStatement(
    new sql.GetStatementRequest()
      .setStatementId(
        esrResponse.getStatementId))


while( Set(
  sql.StatementState.PENDING,
  sql.StatementState.RUNNING)
  .contains(
    gs().getStatus().getState())) {
  println( s"Executing statement ${esrResponse.getStatementId}")
}

println( s"Statement status is ${gs().getStatus()}")

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
