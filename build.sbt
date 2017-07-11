import sbt.Keys.libraryDependencies

resolvers in ThisBuild ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal)
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

name := "CodeFeedr"

version := "0.1-SNAPSHOT"

organization := "org.codefeedr"

scalaVersion in ThisBuild := "2.11.11"

val flinkVersion = "1.3.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file("."))
  .enablePlugins(ScalafmtPlugin)
  .settings(
    libraryDependencies ++= flinkDependencies
  )

libraryDependencies += "codes.reactive" %% "scala-time" % "0.4.1"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.eclipse.mylyn.github" % "org.eclipse.egit.github.core" % "2.1.5" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

mainClass in assembly := Some("org.codefeedr.Job")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
