scalaVersion := "2.11.8"
name := "cs-4240-final"
libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "com.google.guava" % "guava" % "24.1-jre",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.google.cloud" % "google-cloud" % "0.43.0-alpha",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.8.1-hadoop2",
  "org.slf4j" % "slf4j-simple" % "1.8.0-beta2"
)