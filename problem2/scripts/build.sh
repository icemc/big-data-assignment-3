#!/bin/bash

# Ensure SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME is not set."
  exit 1
fi

# Construct classpath from Spark JARs
CLASSPATH=$(find $SPARK_HOME/jars -name "*.jar" | tr '\n' ':')

# Compile Scala sources
mkdir -p ../output
scalac -classpath "$CLASSPATH" ../src/main/scala/*.scala -d ../output

# Package classes into a JAR
cd ../output || exit 1
jar -cvf WeatherPrediction.jar *.class

echo "Build complete: output/WeatherPrediction.jar"