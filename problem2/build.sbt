
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.2.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.4" % "provided",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models"
)

lazy val root = (project in file("."))
  .settings(
    name := "LSAWikipediaMoviePlot"
  )

// Assembly settings
assembly / assemblyJarName := "RunLSA.jar"

// Merge strategy for assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


// Custom task to copy the fat jar to the output/ directory
lazy val assemblyToOutput = taskKey[Unit]("Build fat JAR and copy to output/ directory")

assemblyToOutput := {
  val jar = (Compile / assembly).value
  val outputDir = baseDirectory.value / "output"
  val outputPath = outputDir / jar.getName

  // Create the output directory if it doesn't exist
  IO.createDirectory(outputDir)

  // Copy the fat jar
  IO.copyFile(jar, outputPath)

  println(s"Copied fat jar to $outputPath")
}