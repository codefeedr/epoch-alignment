lazy val root = (project in file("."))
  .aggregate(flink)

lazy val flink = (project in file("flink"))


