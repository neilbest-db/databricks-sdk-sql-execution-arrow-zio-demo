
import com.databricks.sdk
// import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql
// import org.apache.spark.sql.types.StructType
// import scala.collection.JavaConverters._
import zio._

import databricks._
import warehouse._

trait SqlExecutionService {

  // def isWarehouseRunning: ZIO[ Any, Throwable, Boolean]

  def getWarehouseState: ZIO[ Any, Throwable, sql.State]

  def executeStatement( statement: String): ZIO[ Any, Throwable, SqlStatement]

  def getState( sqlStatement: SqlStatement): ZIO[ Any, Throwable, sql.StatementState]

}


object SqlExecutionService {

  // def isWarehouseRunning =
  //   ZIO.serviceWithZIO[ SqlExecutionService](
  //     _.isWarehouseRunning)

  def getWarehouseState =
    ZIO.serviceWithZIO[ SqlExecutionService](
      s => ZIO.attempt( s.getWarehouseState))

  def executeStatement( statement: String) =
    ZIO.serviceWithZIO[ SqlExecutionService](
      s => ZIO.attempt( s.executeStatement( statement)))

  def getState( statement: SqlStatement) =
    ZIO.serviceWithZIO[ SqlExecutionService](
      s => ZIO.attempt( s.getState( statement)))

  val layer: ZLayer[ Any, Nothing, SqlExecutionService] =
    ZLayer.succeed(
      SqlExecution(
        SqlWarehouse(
            "Shared Endpoint",
            WorkspaceClient(
              DatabricksConfig( "e2-demo-west-ws")))
          .start))

}


final case class SqlExecution( sqlWarehouse: SqlWarehouse) extends SqlExecutionService {


  // override def isWarehouseRunning = // : ZIO[ Any, Throwable, Boolean] =
  //   for {
  //     wh <- ZIO.attempt( sqlWarehouse.refresh)
  //     state <- ZIO.attempt( wh.state)
  //     _ <- ZIO.log( s"State is ${state}")
  //   } yield state == sql.State.RUNNING

    // ZIO.attempt( sqlWarehouse.refresh)
    //   .map( _.state == sql.State.RUNNING)

  override def getWarehouseState =
    for {
      wh <- ZIO.attempt( sqlWarehouse.refresh)
      state <- ZIO.attempt( wh.state)
      _ <- ZIO.log( s"Warehouse state is ${state}")
    } yield state

  override def executeStatement( statement: String) = { 
    // val pretty = statement.linesWithSeparators.toSeq.mkString("\t")
    // ZIO.debug( s"Submitting statement:\n\t${pretty}")
    ZIO.attempt( sqlWarehouse.execute( statement))
  }

  override def getState( sqlStatement: SqlStatement) = 
    for {
      state <- ZIO.attempt( sqlStatement.refresh.response.getStatus.getState)
      _ <- ZIO.log( s"Statement execution state is ${state}")
    } yield state

}
