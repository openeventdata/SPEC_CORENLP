name := "TestSpark"

version := "1.0"

scalaVersion := "2.11.8"
//unmanagedBase := baseDirectory.value / "custom_lib"
unmanagedJars in Compile += file("custom_lib/stanford-srparser-2014-10-23-models.jar")


libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.0.0",
                        "org.apache.spark" % "spark-streaming_2.11" % "2.0.0",
                        "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.0",
                        "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0",
                        "org.apache.hadoop" % "hadoop-common" % "2.6.0"  excludeAll(
                        ExclusionRule(organization = "org.eclipse.jetty")),
                        "org.apache.hadoop" % "hadoop-streaming" % "2.6.0",
                        "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
                        "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
                        "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models-spanish",
                        "org.mongodb" %% "casbah" % "2.8.2",
                        "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.1",
                        "org.apache.kafka" % "kafka_2.11" % "0.8.2.1",
                         "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in Compile := Some("KafkaCoreNLP")
