name := "tweets-analyzer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.2.1" % "provided"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" classifier "models"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

    