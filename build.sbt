// PROJECT
name := "test-scala-kafka"
version := "0.1-SNAPSHOT"
organization := "test.scala.stream"

// VERSION
scalaVersion := "2.11.12"
val flinkVersion = "1.7.1"
val xgboostVersion = "0.81"

// Flink dependencies
libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-ml" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion

// XGboost dependencies
// for snapshots add : resolvers += "GitHub Repo" at "https://raw.githubusercontent.com/CodingCat/xgboost/maven-repo/")
// for Windows build and scala console, download the win64 release from https://github.com/criteo-forks/xgboost-jars/releases and copy it to subdir lib/
libraryDependencies += "ml.dmlc" % "xgboost4j" % xgboostVersion

// actually the xgboost4j Flink library is not required at all. Also it relies on Java/Python Rabit implementation for training instead of the Scala one
// libraryDependencies += "ml.dmlc" % "xgboost4j-flink" % xgboostVersion

// Testing libs
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// logging on log4j2 (to be done later)
// libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.1"
// libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
// libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

// SETTINGS
// lazy val root = (project in file("."))


// ASSEMBLY
//  main class 
mainClass in assembly := Some("fr.braux.test.StreamPredict")


// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)


// merge strategy
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
   case x => MergeStrategy.first 
}

// exclude xjboost win64 library
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "xgboost4j-0.81-criteo-20180821_2.11-win64.jar"}
}

// publish assembly
artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

// PUBLISH (update the credentials)
val user = System.getProperty("user.name")
val sftpRes = Resolver.sftp("dockerhost", "work.hostonly.com", "/home/" + user + "/.m2") as (user, user)
publishTo := Some(sftpRes)



