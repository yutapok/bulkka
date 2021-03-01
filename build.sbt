lazy val bulkkaApi = (project in file("bulkka-api"))

lazy val root = (project in file("."))
  .aggregate(bulkkaApi)
  .settings(
    aggregate in run := false
  )
