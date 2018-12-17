import sbt.Keys.libraryDependencies
import sbtassembly.AssemblyPlugin.autoImport.assemblyOption
import sys.process._

lazy val settings = Seq(
  organization := "org.codefeedr",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.12",
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-Xmax-classfile-name","240"
  ),
  assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter {_.data.getName == "shapeless_2.11-2.3.3.jar"}
  },
  test in assembly := {},
  assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(includeScala = false, includeDependency = false)
)

lazy val rootSettings = settings



resolvers in ThisBuild ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal,
  "Artima Maven Repository" at "http://repo.artima.com/releases"
  )


//Object containing all depdencies, to prevent version conflicts between projects
lazy val dependencies = new {
  val flinkV = "1.5.3"

  val zookeeperV = "3.4.12"
  val kafkaV = "2.1.0"

  val scalaAsyncV = "0.9.7"
  val scalaJavaCompatV = "0.8.0"
  val scalaArmV = "2.0"
  val scalaTimeV = "0.4.1"
  val rxV = "0.26.5"
  val typesafeConfigV = "1.3.1"
  val json4sV = "3.6.0-M2"
  val shapelessV = "2.3.3"
  val jodaTimeV = "2.10"

  val scalaLoggingV = "3.5.0"
  val logBackV = "1.2.3"
  val logBackJsonV = "0.1.5"
  val logStashV = "5.2"

  val mockitoV = "2.13.0"
  val scalaTestV = "3.0.1"

  val mongoV = "2.1.0"
  val eclipseV = "2.1.5"

  val flinkScala             = "org.apache.flink"  %% "flink-scala"            % flinkV     % "provided" exclude("org.slf4j","slf4j-log4j12")
  val flinkStreamingScala   = "org.apache.flink"  %% "flink-streaming-scala"  % flinkV       % "provided" exclude("org.slf4j","slf4j-log4j12")
  val flinkTable             = "org.apache.flink"  %% "flink-table"            % flinkV       % "provided" exclude("org.slf4j","slf4j-log4j12")

  val zookeeper = "org.apache.zookeeper" % "zookeeper" % zookeeperV exclude("org.slf4j","slf4j-log4j12")
  val kafka = "org.apache.kafka" % "kafka-clients" % kafkaV

  val scalaAsync = "org.scala-lang.modules" %% "scala-async" % scalaAsyncV
  val scalaJavaCompat = "org.scala-lang.modules" % "scala-java8-compat_2.11" % scalaJavaCompatV
  val scalaArm = "com.jsuereth" %% "scala-arm" % scalaArmV
  val scalaTime =  "codes.reactive" %% "scala-time" % scalaTimeV
  val rx = "io.reactivex" %% "rxscala" % rxV
  val json4sNative = "org.json4s" %% "json4s-native" % json4sV
  val jodaTime = "joda-time" % "joda-time" % jodaTimeV


  //Logging
  val logBack =  "ch.qos.logback" % "logback-classic" % logBackV
  val slfBridge = "org.slf4j" % "log4j-over-slf4j" % "1.7.7"
  val logStash = "net.logstash.logback" % "logstash-logback-encoder" % logStashV

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV
  val scalaP = "org.json4s" % "json4s-scalap_2.11" % json4sV
  val json4sExt = "org.json4s" %% "json4s-ext" % json4sV
  val jackson = "org.json4s" % "json4s-jackson_2.11" % json4sV


  val typeSafeConfig = "com.typesafe" % "config" % typesafeConfigV
  val shapeless = "com.chuusai" %% "shapeless" % shapelessV


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
  dependencies.scalaP,
  dependencies.jackson,
  dependencies.json4sNative,
  dependencies.json4sExt,
  dependencies.typeSafeConfig,
  dependencies.shapeless,
  dependencies.jodaTime,

  //Logging
  dependencies.logStash,
  dependencies.logBack,
  dependencies.scalaLogging,

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
  .dependsOn(codefeedrghtorrent)
  .settings(
    settings,
    parallelExecution in Test := false,
    fork in ThisBuild in Test:= false,
    libraryDependencies ++= commonDependencies ++ zkKafkaDependencies ++ flinkDependencies ++ connectorDependencies

  )

lazy val codefeedrghtorrent = (project in file("codefeedrghtorrent"))
  .settings(
    settings,
    libraryDependencies ++= commonDependencies
  )

lazy val util = (project in file("util"))
  .settings(
    settings,
    libraryDependencies ++= commonDependencies
  )


lazy val demo = (project in file("demo"))
  .dependsOn(flinkintegration,codefeedrghtorrent)
    .settings(
      settings,
 //     mainClass in assembly := Some("org.codefeedr.demo.ghtorrent.GhTorrentUserImporter"),
      assemblyMergeStrategy in assembly := {
        case "log4j.properties" => MergeStrategy.discard
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      },
      libraryDependencies ++= commonDependencies ++ flinkDependencies
    )

//Project containing models
lazy val models = (project in file("models"))
  .dependsOn(util)
  .settings(settings,
    libraryDependencies ++= commonDependencies)

lazy val socketgenerator =  (project in file("evaluation/socketgenerator"))
  .dependsOn(util,models)
  .settings(settings,
      mainClass := Some("org.codefeedr.socketgenerator.Main"),
      libraryDependencies ++= commonDependencies
    )

lazy val socketreceiver =  (project in file("evaluation/socketreceiver"))
  .dependsOn(util,models,flinkintegration)
  .settings(settings,
    mainClass in run := Some("org.codefeedr.socketreceiver.Main"),
    assemblyOption in assembly := (assemblyOption in assembly).value
       .copy(includeScala = false),
    libraryDependencies ++= commonDependencies ++ flinkDependencies
  )

lazy val connectoreval =  (project in file("evaluation/connectoreval"))
  .dependsOn(util,models,flinkintegration)
  .settings(settings,
    libraryDependencies ++= commonDependencies
  )



lazy val mainRunner = project.in(file("mainRunner"))
  .dependsOn(flinkintegration,codefeedrghtorrent,demo,socketreceiver).settings(
  settings,
  // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
  libraryDependencies := (libraryDependencies in flinkintegration).value.map{
    module => module.configurations match {
      case Some("provided") => module.withConfigurations(None)
      case _ => module
    }
  }
)

lazy val execScript = taskKey[Unit]("Execute the shell script")

execScript := {
  "bat test.sh" !
}

lazy val root = (project in file("."))
  .dependsOn(flinkintegration)
  .aggregate(flinkintegration)
    .settings(
      rootSettings
    )

unmanagedJars in Compile += file("lib/flinkwebsocketsource_2.11-1.0.jar")
unmanagedJars in Compile += file("lib/websocketclient-1.0.jar")



// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
    mainClass in (Compile, run),
    runner in (Compile, run))



