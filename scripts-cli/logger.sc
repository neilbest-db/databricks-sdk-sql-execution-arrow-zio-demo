
import scribe.format._

case class setup()

object setup {

  val formatter: Formatter =
    Formatter.fromBlocks(
      groupBySecond(
        cyan( bold( time)), space,
        levelColored, space,
        green( classNameSimple), space,
        italic( threadName), newLine),
      multiLine(
        brightWhite( mdc),
        space,
        messages))

  /*
  val formatter: Formatter =
    Formatter.fromBlocks(
      cyan( bold( time)),
      space,
      levelColored,
      space,
      messages,
      space,
      mdc,
      space,
      green( FormatBlock.ClassAndMethodNameSimple),
      space,
      italic( threadName))
   */

  def apply() = {
  scribe.Logger.minimumLevels(
    // "com.databricks.sdk" -> scribe.Level.Debug,   // show Databricks REST API calls
    "org.apache.hadoop" -> scribe.Level.Info,
    "org.apache.hadoop.io.compress" -> scribe.Level.Warn,
    "org.apache.spark" -> scribe.Level.Error,        // make Spark be quiet!
    "org.apache.spark.ui" -> scribe.Level.Info,
    "org.apache.parquet" -> scribe.Level.Warn,
    "org.sparkproject.jetty" -> scribe.Level.Warn)

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(
      // formatter = scribe.format.Formatter.default,
      formatter = formatter,
      minimumLevel = Some(scribe.Level.Info))
    .replace()

  }
}
