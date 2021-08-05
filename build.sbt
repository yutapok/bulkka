lazy val bulkkaApi = (project in file("bulkka-api"))
  .dependsOn(sjdf % "test->test;compile->compile")

lazy val bulkkaAlone = (project in file("bulkka-standalone"))
  .dependsOn(bulkkaApi % "test->test;compile->compile")

lazy val sjdf = (project in file("s-jdf"))

lazy val root = (project in file("."))
  .aggregate(bulkkaApi, bulkkaAlone, sjdf)
  .settings(
    aggregate in run := false
  )
