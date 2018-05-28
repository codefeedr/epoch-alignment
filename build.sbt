import sbt.Keys.libraryDependencies

lazy val root =
  (project in file("."))
  .settings(settings)
  .aggregate(
    flinkintegration,
    codefeedrghtorrent)


parallelExecution in Test := false

lazy val settings = Seq(
  organization := "org.codefeedr",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.11",
  scalacOptions ++= Seq(
    "-deprecation"
  )
)

resolvers in ThisBuild ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal,
  "Artima Maven Repository" at "http://repo.artima.com/releases"
  )


//Object containing all depdencies, to prevent version conflicts between projects
lazy val dependencies = new {
  val flinkV = "1.4.2"

  val zookeeperV = "3.4.9"
  val kafkaV = "1.0.0"

  val scalaAsyncV = "0.9.7"
  val scalaJavaCompatV = "0.8.0"
  val scalaArmV = "2.0"
  val scalaTimeV = "0.4.1"
  val rxV = "0.26.5"
  val logBackV = "1.1.7"
  val scalaLoggingV = "3.5.0"
  val typesafeConfigV = "1.3.1"
  val json4sV = "3.6.0-M2"

  val mockitoV = "2.13.0"
  val scalaTestV = "3.0.1"

  val mongoV = "2.1.0"
  val eclipseV = "2.1.5"




  val flinkScala             = "org.apache.flink"  %% "flink-scala"            % flinkV       % "provided"
  val flinkStreamingScala   = "org.apache.flink"  %% "flink-streaming-scala"  % flinkV       % "provided"
  val flinkTable             = "org.apache.flink"  %% "flink-table"            % flinkV       % "provided"

  val zookeeper = "org.apache.zookeeper" % "zookeeper" % zookeeperV
  val kafka = "org.apache.kafka" % "kafka-clients" % kafkaV

  val scalaAsync = "org.scala-lang.modules" %% "scala-async" % scalaAsyncV
  val scalaJavaCompat = "org.scala-lang.modules" % "scala-java8-compat_2.11" % scalaJavaCompatV
  val scalaArm = "com.jsuereth" %% "scala-arm" % scalaArmV
  val scalaTime =  "codes.reactive" %% "scala-time" % scalaTimeV
  val rx = "io.reactivex" %% "rxscala" % rxV
  val logBack =  "ch.qos.logback" % "logback-classic" % logBackV
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV
  val scalaP = "org.json4s" % "json4s-scalap_2.11" % json4sV
  val jackson = "org.json4s" % "json4s-jackson_2.11" % json4sV
  val typeSafeConfig = "com.typesafe" % "config" % typesafeConfigV

  val mockito = "org.mockito" % "mockito-core" % mockitoV % "test"
  val scalactic = "org.scalactic" %% "scalactic" % scalaTestV % "test"
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestV % "test"

  val mongo =  "org.mongodb.scala" %% "mongo-scala-driver" % mongoV
  val eclipseGithub = "org.eclipse.mylyn.github" % "org.eclipse.egit.github.core" % eclipseV % "provided"
}


lazy val commonDependencies = Seq(
  //Libraries
  dependencies.scalaAsync,
  dependencies.scalaJavaCompat,
  dependencies.scalaArm,
  dependencies.scalaTime,
  dependencies.rx,
  dependencies.logBack,
  dependencies.scalaLogging,
  dependencies.scalaP,
  dependencies.jackson,
  dependencies.typeSafeConfig,

  //Test
  dependencies.mockito,
  dependencies.scalactic,
  dependencies.scalaTest
)

lazy val zkKafkaDependencies = Seq(
  dependencies.zookeeper,
  dependencies.kafka
)


lazy val flinkDependencies = Seq(
  dependencies.flinkScala,
  dependencies.flinkStreamingScala,
  dependencies.flinkTable
)


lazy val connectorDependencies = Seq(
  dependencies.mongo,
  dependencies.eclipseGithub
)

//TODO: connectorDependencies should not be required in the core project!
lazy val flinkintegration = (project in file("flinkintegration"))
  .settings(
    settings,
    libraryDependencies ++= commonDependencies ++ zkKafkaDependencies ++ flinkDependencies ++ connectorDependencies

  )

lazy val codefeedrghtorrent = (project in file("codefeedrghtorrent"))
  .settings(
    settings,
    libraryDependencies ++= commonDependencies
  )

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
    mainClass in (Compile, run),
   runner in (Compile, run))

// exclude Scala library from assembly
//assemblyOption in assembly := (assemblyOption in assembly).value
//  .copy(includeScala = false)
