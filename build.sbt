lazy val bulkkaApi = (project in file("bulkka-api"))
lazy val bulkkaAlone = (project in file("bulkka-standalone"))
  .dependsOn(bulkkaApi % "test->test;compile->compile")

lazy val root = (project in file("."))
  .aggregate(bulkkaApi, bulkkaAlone)
  .settings(
    aggregate in run := false
  )
