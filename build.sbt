lazy val root = (project in file("."))
  .aggregate(flinkintegration)

lazy val flinkintegration = (project in file("flinkintegration"))


