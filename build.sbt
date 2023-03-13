name := "vigil_assignment"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.698",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.5"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
