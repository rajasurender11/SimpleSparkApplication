
name := "dummy_projects"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "com.typesafe" % "config" % "1.3.1"
)
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val root = (project in file(".")).
  settings(
    name := "dummy_projects"
  ).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

