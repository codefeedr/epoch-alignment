lazy val root = (project in file("."))
  .aggregate(flinkintegration,codefeedrghtorrent,flinkwebsocketsource)

lazy val flinkintegration = (project in file("flinkintegration"))

lazy val codefeedrghtorrent = (project in file("codefeedrghtorrent"))

lazy val flinkwebsocketsource = (project in file("flinkwebsocketsource"))
