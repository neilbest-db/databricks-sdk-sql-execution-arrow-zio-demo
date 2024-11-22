
import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql
import org.apache.spark.sql.types.StructType
import zio.stream.ZStream

import scala.collection.JavaConverters._

import warehouse.SqlWarehouse


case class SqlStatement(

  request: sql.ExecuteStatementRequest,

  response: sql.StatementResponse,

  warehouse: SqlWarehouse

) {

  def statement: String = request.getStatement

  def id: String = response.getStatementId

  def refresh: SqlStatement =
    SqlStatement(
      request,
      warehouse.client.it.statementExecution
        .getStatement(
          new sql.GetStatementRequest()
            .setStatementId( id)),
      warehouse.refresh)

  def schema: StructType =
    StructType.fromDDL(
      response.getManifest.getSchema.getColumns.asScala
        .map( ci => s"${ci.getName} ${ci.getTypeText}")
        .mkString( ",\n"))

  lazy val totalChunkCount: Long =
    response.getManifest.getTotalChunkCount

  def resultChunkN( n: Long): sql.ResultData =
    warehouse.client.it.statementExecution
      .getStatementResultChunkN(
        new sql.GetStatementResultChunkNRequest()
          .setStatementId( id)
          .setChunkIndex( n))
      .setChunkIndex( n)

  def results: ZStream[Any, Throwable, sql.ResultData] =
    ZStream.unfold( 0) {
      case i if i < totalChunkCount => Some( resultChunkN( i) -> (i + 1))
      case _ => None
    } bufferUnbounded
 
}
