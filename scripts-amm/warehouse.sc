import $ivy.`org.apache.spark::spark-sql:3.4.0`
// import $ivy.`org.apache.spark::spark-core:3.4.0`
import $ivy.`com.databricks:databricks-sdk-java:0.34.0`
import $ivy.`dev.zio::zio:2.1.11`

import com.databricks.sdk
import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

import zio._

import $file.databricks, databricks._

case class SqlWarehouse(
  it:     sql.GetWarehouseResponse,
  client: WorkspaceClient) {

  def id: String = it.getId

  def name: String = it.getName

  def start: SqlWarehouse = {
    client.it.warehouses.start( id).getResponse
    // don't wait or capture because response type is `Void`
    refresh
  }

  def refresh: SqlWarehouse =
    SqlWarehouse( client.it.warehouses.get( id), client)

  def state: sql.State = it.getState
 

  // def stop(): sql.State

  // def execute( statement: String): sql.StatementResponse

  def request( statement: String) =
    new sql.ExecuteStatementRequest()
      .setWarehouseId( id)
      .setCatalog( "system")
      .setSchema( "access")
      .setDisposition( sql.Disposition.EXTERNAL_LINKS)
      .setFormat( sql.Format.ARROW_STREAM)
      .setStatement( statement)


  def response( request: sql.ExecuteStatementRequest): sql.StatementResponse =
      client.it.statementExecution.executeStatement( request)


  def execute( statement: String): SqlStatement = {
    val req = request( statement)
    val res = response( req)
    SqlStatement( req, res, refresh)
  }

}

object SqlWarehouse {

  def apply( name: String, client: WorkspaceClient): SqlWarehouse = {

    val endpointInfo: Option[ sql.EndpointInfo] =
      client.it.warehouses
        .list( new sql.ListWarehousesRequest())
        .asScala
        .collectFirst {
          case ei if ei.getName() == name => ei
        }

    endpointInfo match {
      case Some( ei) =>
        apply(
          client.it.warehouses.get( ei.getId()),
          client)
      case None =>
        throw new DatabricksException( s"Warehouse '${name}' not found.")
    }

  }

  def layer( name: String, client: WorkspaceClient): ZLayer[ Any, Nothing, SqlWarehouse] =
    ZLayer.succeed( apply( name, client).start)

}

case class SqlStatement(
  request: sql.ExecuteStatementRequest,
  response: sql.StatementResponse,
  // status: sql.GetStatementResponse,
  warehouse: SqlWarehouse) {
  // links: Iterator[sql.ExternalLink], schema: StructType)

  def statement: String = request.getStatement

  def id: String = response.getStatementId

  def refresh: SqlStatement =
    SqlStatement(
      request,
      warehouse.client.it.statementExecution
        .getStatement(
          new sql.GetStatementRequest().setStatementId( id)),
        // .getStatus
        // .getState
      warehouse.refresh)

  def links: Iterable[ sql.ExternalLink] =
    response.getResult.getExternalLinks.asScala
  //.toParArray //.iterator.asScala

  def schema: StructType =
    StructType.fromDDL(
      response.getManifest.getSchema.getColumns.asScala
        .map( ci => s"${ci.getName} ${ci.getTypeText}")
        .mkString( ",\n"))


  def result: SqlStatementResult =
    SqlStatementResult( links, schema)

}

case class SqlStatementResult(
  links: Iterable[ sql.ExternalLink],
  schema: StructType)
