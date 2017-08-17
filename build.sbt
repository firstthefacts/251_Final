name := "251final"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"


