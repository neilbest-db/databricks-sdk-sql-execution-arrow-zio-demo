import $cp.^.arrow
// import spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.{
  PublicArrowConverters => ArrowConverters
}

import $ivy.`org.apache.spark::spark-sql:3.4.0`
import $ivy.`org.apache.spark::spark-core:3.4.0`
import $ivy.`org.slf4j:slf4j-api:2.0.12`
import $ivy.`org.slf4j:slf4j-simple:2.0.12`
import $ivy.`io.delta:delta-core_2.12:2.4.0`
import $ivy.`com.databricks:databricks-sdk-java:0.19.0`
// import $ivy.`dev.zio::zio:2.1.9`
// import $ivy.`dev.zio::zio-http:3.0.1`

// import $ivy.`io.netty:netty-buffer:4.1.110.Final`


import com.databricks.sdk
import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

/*
import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.{
  DatabricksConfig,
  DatabricksError
}
/*
import com.databricks.sdk.service.sql.{  
  Disposition,
  ExecuteStatementRequest,
  Format,
  GetWarehouseRequest,
  ListWarehousesRequest,
  State
 }
 */
 */

// import grizzled.slf4j.Logger

import org.apache.spark.sql.SparkSession

import org.slf4j.{Logger,LoggerFactory}
import org.slf4j.simple.SimpleLogger
// import zio._
// import zio.http._

import scala.collection.JavaConverters._

import java.time.Duration



/*

 */

/*
      """SELECT DISTINCT
        |  coalesce(
        |    request_params.cluster_id
        |    request_params.clusterId)
        |    AS cluster_id
        |FROM audit
        |WHERE service_name = 'clusters'
        |  AND action_name IN ('create', 'edit')
        |  AND event_date = '2024-09-01'
        |;""".stripMargin

   // AND request_params.cluster_log_conf NOT NULL

 */

// configure logging

val sysProps = new sys.SystemProperties()

Map(
  "slf4j.provider" ->
    "org.slf4j.simple.SimpleLogger",
  SimpleLogger.DEFAULT_LOG_LEVEL_KEY ->
    "INFO",
  SimpleLogger.SHOW_DATE_TIME_KEY ->
    "true")
  .foreach { case (k,v) => sysProps.put(k,v) }

val logger = LoggerFactory.getLogger( "my-logger")

logger.info( "Logger instantiated")


trait DatabricksConfigSDK {

  self: DatabricksConfig =>

  def profile(): String = self.it.getProfile()

}

case class DatabricksConfig( it: sdk.core.DatabricksConfig) extends DatabricksConfigSDK

object DatabricksConfig {

  def apply( profile: String): DatabricksConfig =
    apply( new sdk.core.DatabricksConfig().setProfile( profile).resolve)


  // val live: RLayer[ String, DatabricksConfig] =
  //   ZLayer.fromFunction( DatabricksConfig.apply(_))

}

trait WorkspaceClientSDK {

  self: WorkspaceClient =>

  def config(): DatabricksConfig =
    DatabricksConfig( self.it.config())

  // throws DatabricksException --> `az login`
  def me(): sdk.service.iam.User =
    self.it.currentUser.me()

}

case class WorkspaceClient( it: sdk.WorkspaceClient) extends WorkspaceClientSDK

object WorkspaceClient {

  def apply( config: DatabricksConfig): WorkspaceClient =
    WorkspaceClient( new sdk.WorkspaceClient( config.it))

  // val live: RLayer[ DatabricksConfig, WorkspaceClient] =
  //   ZLayer.fromFunction( WorkspaceClient.apply(_))

}


trait SqlWarehouseSDK {

  self: SqlWarehouse =>

  def start(): SqlWarehouse = {
    self.client.it.warehouses
      .start( self.it.getId())
      // .get( Duration.ZERO)
      // .get( Duration.ofMinutes(20)) // default
      .getResponse()  // don't wait, but type is Void
    self.refresh()
  }

  def refresh(): SqlWarehouse =
    SqlWarehouse(
      self.client.it.warehouses
        .get( self.it.getId()),
      self.client)

  // def stop(): sql.State

  // def execute( statement: String): sql.StatementResponse

}

case class SqlWarehouse(
  it:     sql.GetWarehouseResponse,
  client: WorkspaceClient
) extends SqlWarehouseSDK {

  def id = it.getId()

}

object SqlWarehouse {

  def apply(
    name: String,            // e.g. "General_Warehouse", "Shared Endpoint"
    client: WorkspaceClient
  ): SqlWarehouse = {

    val endpointInfo: Option[ sql.EndpointInfo] =
      client.it.warehouses
        .list( new sql.ListWarehousesRequest())
        .asScala
        .collectFirst {
          case ei if ei.getName() == name => ei
        }

    // val it: Option[ sql.GetWarehouseResponse] =

    endpointInfo match {
      case Some( ei) =>
        apply(
          client.it.warehouses.get( ei.getId()),
          client)
      case None =>
        throw new DatabricksException( s"Warehouse '${name}' not found.")
    }

  }

  
 // val layer: RLayer[ WorkspaceClient, SqlWarehouseService] =
 //   ZLayer.fromFunction( SqlWarehouse.apply


  // override def getId( name: String): RIO[ WorkspaceClient, String] 
}

/*
object SqlWarehouseService {

  // override def get() 

  // def client(): RLayer[ SqlWarehouseService, WorkspaceClient] =
  //   ZLayer.service[SqlWareHouseService].project(_.client)

  override def start(): UIO[ SqlWarehouseService, sql.GetWarehouseResponse] = {
    for

  }

  def refresh() =
    this.copy( bean = client.warehouses.get( id))

  // def stop() = ???

  // def execute( statement) = ???

}
 */

val w = WorkspaceClient( DatabricksConfig( "e2-demo-west-ws"))

// val sqlWh = () => SqlWarehouse( "Shared Endpoint", w).start
val sqlWh = SqlWarehouse( "Shared Endpoint", w).start

val sqlWhState = () => sqlWh.refresh.it.getState() 

while ( sqlWhState() != sql.State.RUNNING) {
  println( s"Waiting for SQL Warehouse '${sqlWh.it.getName()}' to start . . .")
  Thread.sleep( 3000)
}

// assert(
//   sqlWh()
//     .start()
//     .it
//     .getState()
//     == sql.State.RUNNING)

println( s"${sqlWh.it.getName()} state is: ${sqlWhState()}")

val esr = new sql.ExecuteStatementRequest()
    .setWarehouseId( sqlWh.id)  //.it.getId())
    .setCatalog( "system")
    .setSchema( "access")
    .setDisposition( com.databricks.sdk.service.sql.Disposition.EXTERNAL_LINKS)
    .setFormat( com.databricks.sdk.service.sql.Format.ARROW_STREAM)
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
