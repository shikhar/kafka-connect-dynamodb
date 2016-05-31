name := "kafka-connect-dynamodb"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "connect-api" % "0.10.0.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.5"
