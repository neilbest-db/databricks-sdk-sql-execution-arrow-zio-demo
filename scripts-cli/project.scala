
//> using scala 2.12.18

// //> using resourceDir ./resources


// # Databricks SDK
//
//> using dep "com.databricks:databricks-sdk-java:0.34.0,exclude=com.fasterxml.jackson.core%jackson-databind"


// # HTTP client
//
//> using dep com.lihaoyi::requests:0.9.0


// # OS-Lib
//
//> using dep com.lihaoyi::os-lib:0.11.3


// # logging
//
//> using dep com.outr::scribe:3.15.2
//> using dep com.outr::scribe-slf4j:3.15.2
//> using dep com.outr::scribe-slf4j2:3.15.2
// //> using dep org.slf4j:slf4j-api:2.0.9
//> using dep org.slf4j:slf4j-api:1.7.35
//> using dep org.apache.logging.log4j:log4j-slf4j-impl:2.17.2
// //> using dep org.apache.logging.log4j:log4j-to-slf4j:2.19.0


// # ZIO concurrency framework
//
//> using dep dev.zio::zio:2.1.11
//> using dep dev.zio::zio-logging-slf4j:2.4.0
// //> using dep dev.zio::zio-logging-slf4j2-bridge:2.4.0
//> using dep dev.zio::zio-streams:2.1.11


// # ZIO Spark (experimental)
//
//> using repositories sonatype:snapshots
//> using dep io.univalence::zio-spark:0.13.0+00007-7a4881e6-SNAPSHOT


// # Delta tables
//
// compatibility info: https://docs.delta.io/latest/releases.html
//
// //> using dep io.delta:delta-core_2.12:2.4.0 // Spark == 3.4.x
//> using dep io.delta:delta-core_2.12:2.3.0 // Spark = 3.3.x


// # Apache Spark
//
// //> using dep "org.apache.spark::spark-core:3.4.0,exclude=org.apache.logging.log4j%log4j-slf4j-impl"
// //> using dep "org.apache.spark::spark-sql:3.4.0,exclude=org.apache.logging.log4j%log4j-slf4j-impl"
//> using dep org.apache.spark::spark-core:3.3.2
//> using dep org.apache.spark::spark-sql:3.3.2




