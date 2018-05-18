ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "FlinkWebSocketSource"

version := "0.1"

organization := "net.vankaam"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.4.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")


//See: https://doc.akka.io/docs/akka-http/current/compatibility-guidelines.html
val akkaVersion = "2.5.12"
val akkaHttpVersion = "10.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor"  % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
// If testkit used, explicitly declare dependency on akka-streams-testkit in same version as akka-actor
libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit"   % akkaHttpVersion % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion     % Test

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0",

  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

  "org.mockito" % "mockito-core" % "2.13.0" % "test",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)




lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("net.vankaam.Job")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
