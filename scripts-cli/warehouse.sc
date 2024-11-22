
import com.databricks.sdk
import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql
import zio._
import zio.stream.ZStream

import scala.collection.JavaConverters._

import databricks._
import statement.SqlStatement

case class SqlWarehouse(

  it:     sql.GetWarehouseResponse,

  client: WorkspaceClient

) {

  def id: String = it.getId

  def name: String = it.getName

  def start: SqlWarehouse = {
    // initial response type is `Void`
    client.it.warehouses.start( id).getResponse
    refresh
  }

  // def stop(): sql.State

  def refresh: SqlWarehouse =
    SqlWarehouse( client.it.warehouses.get( id), client)

  def state: sql.State = it.getState

  def request( statement: String) =
    new sql.ExecuteStatementRequest()
      .setWarehouseId( id)
      .setCatalog( "system")
      .setSchema( "access")
      .setDisposition( sql.Disposition.EXTERNAL_LINKS)
      .setFormat( sql.Format.ARROW_STREAM)
      .setWaitTimeout( "0s")
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
}
