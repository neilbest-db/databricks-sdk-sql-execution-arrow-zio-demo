import $ivy.`com.databricks:databricks-sdk-java:0.34.0`
// import $ivy.`dev.zio::zio:2.1.11`

import com.databricks.sdk
import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.service.sql

import scala.collection.JavaConverters._

// import zio._

/*
trait DatabricksConfigSDK {

  self: DatabricksConfig =>

  def profile(): String = self.it.getProfile()

}
 */


case class DatabricksConfig( it: sdk.core.DatabricksConfig) {
  // extends DatabricksConfigSDK

  def profile(): String = it.getProfile()

}

object DatabricksConfig {

  def apply( profile: String): DatabricksConfig =
    apply( new sdk.core.DatabricksConfig().setProfile( profile).resolve)

  // def config: ZIO[ DatabricksConfig,


}

case class WorkspaceClient( it: sdk.WorkspaceClient) {

  def config(): DatabricksConfig =
    DatabricksConfig( it.config())

  // throws DatabricksException --> `az login`
  def me(): sdk.service.iam.User =
    it.currentUser.me()

}

object WorkspaceClient {

  def apply( config: DatabricksConfig): WorkspaceClient =
    apply( new sdk.WorkspaceClient( config.it))

  /*
  def layer( profile: String): ZLayer[ Any, Throwable, WorkspaceClient] =
    ZLayer {
      ZIO.attempt( apply( DatabricksConfig( profile)))
    }
   */

}
