name := "Paris"

version := "1.0"

scalaVersion := "2.11.11"

assemblyJarName in assembly := "Paris.jar"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
  ,"org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.11"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.11"

autoScalaLibrary := false

target in (Compile, doc) := baseDirectory.value / "api"