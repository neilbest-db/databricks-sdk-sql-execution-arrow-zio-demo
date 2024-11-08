import mill._, scalalib._

object arrow extends RootModule with ScalaModule {

  def zincIncrementalCompilation = false

  def scalaVersion = "2.12.18"

  def ammoniteVersion = "2.5.11"

  def compileIvyDeps = Agg(
    ivy"org.apache.spark::spark-sql:3.4.0",
    ivy"org.apache.spark::spark-core:3.4.0"
  )

  override def assemblyRules =
    Assembly.defaultRules ++
    Seq( "scala/.*", "org\\.apache\\.spark/.*")
      .map( Assembly.Rule.ExcludePattern.apply)

  def assembly = T {
    // val dest = T.dest / s"${artifactName()}-${publishVersion()}-assembly.jar"
    // val dest = T.dest / s"arrow.jar"
    val dest = os.pwd / s"arrow.jar"
    os.copy(super.assembly().path, dest)
    PathRef(dest)
  }

}
