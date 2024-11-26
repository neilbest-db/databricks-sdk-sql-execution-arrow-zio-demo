import com.databricks.sdk.service.sql

// import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{
  DataFrame,
  PublicArrowConverters => ArrowConverters }
import org.apache.spark.sql.types.StructType

import zio._
import zio.logging.LogAnnotation
import zio.spark.rdd.RDD
import zio.spark.sql.{ fromSpark, SparkSession}
import zio.stream.{ ZStream, ZPipeline, ZSink}

import java.io.InputStream
import java.nio.channels.Channels.newChannel

object ArrowPipeline {

  type ArrowBatch = Array[ Byte]

  def index( label: String) = LogAnnotation[ Long]( label, (_, i) => i, _.toString)

  val download: ZPipeline[ Any, Throwable, (sql.ExternalLink, Long), (geny.Readable, Long)] =
    ZPipeline.mapZIOPar(12) {
      case ( link, i) =>
        ZIO.log( "Downloading result chunk") @@ index( "i")( i) *>
        ZIO.attempt( ( requests.get.stream( link.getExternalLink), i))
    }

  // TODO: throws requests.TimeoutException ("Request to https://<link>?<REDACTME> timed out. (readTimeout: 10000, connectTimout: 10000)")
  // TODO: throws requests.RequestFailedException ("403 . . . expired")


  def getBatchesFromStream( stream: geny.Readable): Iterator[ ArrowBatch] =
    ArrowConverters.getBatchesFromStream(
      stream.readBytesThrough( (is: InputStream) => newChannel( is)))

  val arrowBatches: ZPipeline[ Any, Throwable, (geny.Readable, Long), (Iterator[ ArrowBatch], Long)] =
    ZPipeline.mapZIOParUnordered(12) {
      case (stream, i) =>
        ZIO.log( "Decoding Arrow batches") @@ index( "i")( i) *>
        ZIO.attempt( ( getBatchesFromStream( stream), i))
    }

  val rdd: ZPipeline[ SparkSession, Throwable, (Iterator[ ArrowBatch], Long), (RDD[ ArrowBatch], Long)] =
    ZPipeline.mapZIOParUnordered(12) {
      case (batches, i) => for {
        spark <- ZIO.service[ SparkSession]
        _ <- ZIO.log( "Creating Arrow batch RDD") @@ index( "i")( i)
        rdd = spark.sparkContext.makeRDD( batches.toSeq)  // batches.iterator),  // Spark > 3.3.2 (?)
      } yield (rdd, i)
    }

  def appendToDelta( schema: StructType, location: os.Path): ZPipeline[ SparkSession, Throwable, (RDD[ ArrowBatch], Long), Long] =
    ZPipeline.mapZIOParUnordered(12) {
      case (rdd, i) =>  for {
        spark <- ZIO.service[ SparkSession]
        df <- fromSpark { spark => ArrowConverters.toDataFrame( rdd.underlying.toJavaRDD, schema.json, spark) }
        n_i = df.count
        _ <- ZIO.log( s"Writing $n_i records to Delta") @@ index( "i")( i)
        _ <- ZIO.attempt( df.write.format("delta").mode("append").save( location.toString))
      } yield n_i
    }

  val countRows: ZSink[ Any, Throwable, Long, Nothing, Long] =
    ZSink.sum[Long]

}
