val AkkaVersion = "2.6.10"
libraryDependencies ++= Seq(
 "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
 "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "2.0.2",
 "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
 "org.json4s" %% "json4s-jackson" % "3.6.5",
)
