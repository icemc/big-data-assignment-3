import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.rdd.RDD

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object WeatherPrediction {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("WeatherPrediction")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    // Part (a): Data Parsing and Preparation
    def parseNOAA(rawData: RDD[String]): RDD[Array[Any]] = {
      rawData
        .filter(line => line.substring(87, 92) != "+9999") // filter out missing temperature labels
        .map { line =>
          val year = line.substring(15, 19).toInt
          val month = line.substring(19, 21).toInt
          val day = line.substring(21, 23).toInt
          val hour = line.substring(23, 25).toInt
          val latitude = line.substring(28, 34).toDouble / 1000
          val longitude = line.substring(34, 41).toDouble / 1000
          val elevationDimension = line.substring(46, 51).toInt
          val directionAngle = line.substring(60, 63).toInt
          val speedRate = line.substring(65, 69).toDouble / 10
          val ceilingHeightDimension = line.substring(70, 75).toInt
          val distanceDimension = line.substring(78, 84).toInt
          val dewPointTemperature = line.substring(93, 98).toDouble / 10
          val airTemperature = line.substring(87, 92).toDouble / 10

          Array(year, month, day, hour, latitude, longitude, elevationDimension,
            directionAngle, speedRate, ceilingHeightDimension, distanceDimension,
            dewPointTemperature, airTemperature)
        }
    }

    // Define schema for our DataFrame
    val schema = StructType(Array(
      StructField("year", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("hour", IntegerType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("elevation", IntegerType, nullable = true),
      StructField("wind_direction", IntegerType, nullable = true),
      StructField("wind_speed", DoubleType, nullable = true),
      StructField("ceiling_height", IntegerType, nullable = true),
      StructField("visibility", IntegerType, nullable = true),
      StructField("dew_point", DoubleType, nullable = true),
      StructField("air_temperature", DoubleType, nullable = true)
    ))

    // Handle input path (default or user-provided)
    val inputPath = if (args.length > 0) args(0) else "../../input/NOAA-065900/065900*"
    println(s"Using input path: $inputPath")

    // Parse the data and create DataFrame
    val rawNOAA = sc.textFile(inputPath)
    val parsedNOAA = parseNOAA(rawNOAA)

    // Convert RDD to DataFrame with proper schema
    val weatherDF = spark.createDataFrame(
      parsedNOAA.map(arr =>
        Row(
          arr(0).asInstanceOf[Int],
          arr(1).asInstanceOf[Int],
          arr(2).asInstanceOf[Int],
          arr(3).asInstanceOf[Int],
          arr(4).asInstanceOf[Double],
          arr(5).asInstanceOf[Double],
          arr(6).asInstanceOf[Int],
          arr(7).asInstanceOf[Int],
          arr(8).asInstanceOf[Double],
          arr(9).asInstanceOf[Int],
          arr(10).asInstanceOf[Int],
          arr(11).asInstanceOf[Double],
          arr(12).asInstanceOf[Double]
        )
      ),
      schema
    )

    // Cache the DataFrame for performance
    weatherDF.cache()
    val totalRecords = weatherDF.count()
    println(s"Total records parsed: $totalRecords")

    // Show sample data
    println("Sample data:")
    weatherDF.show(10)

    // Prepare feature vector
    val featureCols = Array("year", "month", "day", "hour", "latitude", "longitude",
      "elevation", "wind_direction", "wind_speed", "ceiling_height",
      "visibility", "dew_point")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    // Split data into training (1949-2023), validation (2024), and test (2025)
    val trainData = weatherDF.filter(col("year") <= 2023)
    val validData = weatherDF.filter(col("year") === 2024)
    val testData = weatherDF.filter(col("year") === 2025)

    println(s"Training data count (1949-2023): ${trainData.count()}")
    println(s"Validation data count (2024): ${validData.count()}")
    println(s"Test data count (2025): ${testData.count()}")

    // Part (b): Linear Regression Model
    println("\nTraining Linear Regression model...")
    val lrStartTime = System.nanoTime()
    val lr = new LinearRegression()
      .setLabelCol("air_temperature")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrPipeline = new Pipeline()
      .setStages(Array(assembler, lr))

    val lrModel = lrPipeline.fit(trainData)
    val lrElapsedTime = (System.nanoTime() - lrStartTime) / 1e9

    // Evaluate on validation set
    val lrValidPredictions = lrModel.transform(validData)
    val evaluator = new RegressionEvaluator()
      .setLabelCol("air_temperature")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val lrValidRmse = evaluator.evaluate(lrValidPredictions)
    println(s"Linear Regression RMSE on validation data (2024) = $lrValidRmse")

    // Part (c): Random Forest Regressor
    println("\nTraining Random Forest model...")
    val rfStartTime = System.nanoTime()
    val rf = new RandomForestRegressor()
      .setLabelCol("air_temperature")
      .setFeaturesCol("features")
      .setNumTrees(10)
      .setMaxDepth(5)

    val rfPipeline = new Pipeline()
      .setStages(Array(assembler, rf))

    val rfModel = rfPipeline.fit(trainData)
    val rfElapsedTime = (System.nanoTime() - rfStartTime) / 1e9

    // Evaluate on validation set
    val rfValidPredictions = rfModel.transform(validData)
    val rfValidRmse = evaluator.evaluate(rfValidPredictions)
    println(s"Random Forest RMSE on validation data (2024) = $rfValidRmse")

    // Part (d): Model Comparison on 2025 Test Data
    println("\nEvaluating models on test data (2025)...")
    val lrTestPredictions = lrModel.transform(testData)
    val rfTestPredictions = rfModel.transform(testData)

    val lrTestRmse = evaluator.evaluate(lrTestPredictions)
    val rfTestRmse = evaluator.evaluate(rfTestPredictions)

    println(s"Linear Regression RMSE on test data (2025) = $lrTestRmse")
    println(s"Random Forest RMSE on test data (2025) = $rfTestRmse")

    // Show feature importance for Random Forest
    val rfModelStage = rfModel.stages.last.asInstanceOf[RandomForestRegressionModel]
    println("\nRandom Forest Feature Importances:")
    rfModelStage.featureImportances.toArray.zip(featureCols)
      .sortBy(-_._1)
      .foreach { case (importance, feature) =>
        println(s"$feature: $importance")
      }

    // Part (e): Correlation Analysis
    println("\nCalculating correlations with air temperature...")
    val correlations = featureCols.map { colName =>
      (colName, weatherDF.stat.corr("air_temperature", colName))
    }

    println("\nCorrelations with air temperature:")
    correlations.sortBy { case (_, corr) => -math.abs(corr) }.foreach { case (colName, corr) =>
      println(f"$colName%-15s $corr%.4f")
    }

    val maxCorrFeature = correlations.maxBy { case (_, corr) => math.abs(corr) }._1

    // Save results to Problem_2.txt
    // Build comprehensive results string
    val results =
      s"""
=== Weather Prediction Analysis Report ===
Generated: ${java.time.LocalDateTime.now()}

1. DATA SUMMARY:
----------------
- Total records processed: $totalRecords
- Training period: 1949-2023 (${trainData.count()} records)
- Validation year: 2024 (${validData.count()} records)
- Test year: 2025 (${testData.count()} records)

2. MODEL PERFORMANCE:
--------------------
Linear Regression:
- Validation RMSE (2024): $lrValidRmse
- Test RMSE (2025): $lrTestRmse
- Training time: ${"%.2f".format(lrElapsedTime)} seconds

Random Forest:
- Validation RMSE (2024): $rfValidRmse
- Test RMSE (2025): $rfTestRmse
- Training time: ${"%.2f".format(rfElapsedTime)} seconds

3. PERFORMANCE COMPARISON:
-------------------------
Random Forest was ${if ((rfElapsedTime/lrElapsedTime) > 1) "slower" else "faster"} to train
${
        if (rfTestRmse < lrTestRmse)
          "but achieved better accuracy"
        else
          "without accuracy improvement"
      }

4. FEATURE ANALYSIS:
-------------------
Feature Importances (Random Forest):
${
        rfModelStage.featureImportances.toArray.zip(featureCols)
          .sortBy(-_._1)
          .map { case (imp, feat) => f"  - $feat%-15s ${imp}%.4f" }
          .mkString("\n")
      }

Temperature Correlations:
${
        correlations.sortBy { case (_, corr) => -math.abs(corr) }
          .map { case (colName, corr) => f"  - $colName%-15s $corr%.4f" }
          .mkString("\n")
      }

Strongest Correlation: $maxCorrFeature (${correlations.find(_._1 == maxCorrFeature).get._2})

5. OBSERVATIONS:
---------------
${
        if (rfTestRmse < lrTestRmse)
          "The Random Forest model outperformed Linear Regression"
        else
          "Linear Regression performed better than Random Forest"
      }

Key Findings:
1. The most predictive feature was ${rfModelStage.featureImportances.toArray.zip(featureCols).maxBy(_._1)._2}
2. Temperature shows strongest correlation with $maxCorrFeature
3. Model performance difference: ${math.abs(rfTestRmse - lrTestRmse)} RMSE

6. SAMPLE PREDICTIONS:
---------------------
First 10 test predictions (Linear Regression):
----------------------------------------------
${
        lrTestPredictions.select("year", "month", "day", "hour", "air_temperature", "prediction")
          .limit(10)
          .collect()
          .map(row => f"${row.getInt(1)}/${row.getInt(2)}/${row.getInt(0)} ${row.getInt(3)}:00 | Actual: ${row.getDouble(4)}%.1f°C | Predicted: ${row.getDouble(5)}%.1f°C")
          .mkString("\n")
      }


First 10 test predictions (Random Forest):
${
        rfTestPredictions.select("year", "month", "day", "hour", "air_temperature", "prediction")
          .limit(10)
          .collect()
          .map(row => f"${row.getInt(1)}/${row.getInt(2)}/${row.getInt(0)} ${row.getInt(3)}:00 | Actual: ${row.getDouble(4)}%.1f°C | Predicted: ${row.getDouble(5)}%.1f°C")
          .mkString("\n")
      }
"""

    // Write results to file
    Files.write(Paths.get("report.txt"), results.getBytes(StandardCharsets.UTF_8))
    println("\nResults saved to report.txt")

    spark.stop()
  }
}