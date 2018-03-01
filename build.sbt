//import sbtassembly.AssemblyPlugin.autoImport.{ShadeRule, assemblyShadeRules}

import sbtassembly.AssemblyPlugin.autoImport._


name := "CustomerActivity"

version := "3.0"

scalaVersion := "2.11.0"

val ElasticV = "5.5.2"


libraryDependencies += "org.elasticsearch" % "elasticsearch" % "5.5.2"

libraryDependencies +="org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"

libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.0"
libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.5.0"


libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}