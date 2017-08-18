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
  .settings(
    libraryDependencies ++= flinkDependencies
  ).aggregate(core)


lazy val core = (project.dependsOn(model) in file("Core"))
  .settings(
    name := "core"
  )

lazy val model = (project in file("Model"))
  .settings(
    name := "Model"
  )

libraryDependencies += "codes.reactive" %% "scala-time" % "0.4.1"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.eclipse.mylyn.github" % "org.eclipse.egit.github.core" % "2.1.5" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.0"
libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0"
libraryDependencies += "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.8.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.6"

//Twitters zookeeper client
libraryDependencies += "com.twitter" %% "util-zk" % "6.45.0"

mainClass in assembly := Some("org.codefeedr.Job")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
