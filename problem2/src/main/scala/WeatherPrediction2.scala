import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.rdd._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object WeatherPrediction2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("WeatherPrediction")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext


    // Parse NOAA data using the provided function
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

          Array(year, month, day, hour, latitude, longitude, elevationDimension, directionAngle,
            speedRate, ceilingHeightDimension, distanceDimension, dewPointTemperature, airTemperature)
        }
    }

    // Load and parse the data
    val startTime = System.currentTimeMillis()

    // Handle input path (default or user-provided)
    val inputPath = if (args.length > 0) args(0) else "../../input/NOAA-065900/065900*"
    println(s"Using input path: $inputPath")

    val rawNOAA = spark.sparkContext.textFile(inputPath)
    val parsedNOAA = parseNOAA(rawNOAA)

    // Cache the RDD to avoid reprocessing
    parsedNOAA.cache()
    val count = parsedNOAA.count()
    println(s"Number of parsed records: $count")

    // Convert to DataFrame with appropriate column names
    val colNames = Array("year", "month", "day", "hour", "latitude", "longitude",
      "elevation", "wind_direction", "wind_speed",
      "ceiling_height", "visibility", "dew_point", "air_temperature")

    val weatherDF = parsedNOAA.map { arr =>
      (arr(0).asInstanceOf[Int], arr(1).asInstanceOf[Int], arr(2).asInstanceOf[Int],
        arr(3).asInstanceOf[Int], arr(4).asInstanceOf[Double], arr(5).asInstanceOf[Double],
        arr(6).asInstanceOf[Int], arr(7).asInstanceOf[Int], arr(8).asInstanceOf[Double],
        arr(9).asInstanceOf[Int], arr(10).asInstanceOf[Int], arr(11).asInstanceOf[Double],
        arr(12).asInstanceOf[Double])
    }.toDF(colNames: _*)

    // Show the first few records
    weatherDF.show(5)
    println(s"Total records: ${weatherDF.count()}")

    // Prepare feature vector
    val featureCols = Array("year", "month", "day", "hour", "latitude", "longitude",
      "elevation", "wind_direction", "wind_speed",
      "ceiling_height", "visibility", "dew_point")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val featureDF = assembler.transform(weatherDF)

    // Split data: training (1949-2023), validation (2024), test (2025)
    val trainingData = featureDF.filter($"year" < 2024)
    val validationData = featureDF.filter($"year" === 2024)
    val testData = featureDF.filter($"year" === 2025)

    val trainingCount = trainingData.count()
    val validationCount = validationData.count()
    val testCount = testData.count()

    println(s"Training data count (1949-2023): $trainingCount")
    println(s"Validation data count (2024): $validationCount")
    println(s"Test data count (2025): $testCount")

    // Define Linear Regression model
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("air_temperature")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Train the Linear Regression model and time it
    val lrTrainStart = System.currentTimeMillis()
    val lrModel = lr.fit(trainingData)
    val lrTrainTime = (System.currentTimeMillis() - lrTrainStart) / 1000.0

    // Print coefficients and intercept
    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept}")

    // Evaluate on validation data
    val lrPredictions = lrModel.transform(validationData)
    val lrEvaluator = new RegressionEvaluator()
      .setLabelCol("air_temperature")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val lrValidationRMSE = lrEvaluator.evaluate(lrPredictions)
    println(s"Linear Regression RMSE on validation data = $lrValidationRMSE")

    // Define Random Forest Regressor
    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("air_temperature")
      .setNumTrees(10)  // as specified in the assignment

    // Train the Random Forest model and time it
    val rfTrainStart = System.currentTimeMillis()
    val rfModel = rf.fit(trainingData)
    val rfTrainTime = (System.currentTimeMillis() - rfTrainStart) / 1000.0

    // Evaluate Random Forest on validation data
    val rfPredictions = rfModel.transform(validationData)
    val rfEvaluator = new RegressionEvaluator()
      .setLabelCol("air_temperature")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rfValidationRMSE = rfEvaluator.evaluate(rfPredictions)
    println(s"Random Forest RMSE on validation data = $rfValidationRMSE")

    // Extract and format feature importances for Random Forest
    val featureImportances = rfModel.featureImportances.toArray
    val featureImportanceMap = featureCols.zip(featureImportances).sortBy(-_._2)
    println("Random Forest Feature Importances:")
    featureImportanceMap.foreach { case (feature, importance) =>
      println(f"$feature: $importance%.4f")
    }

    // Predict with Linear Regression model on 2025 test data
    val lrTestPredictions = lrModel.transform(testData)
    val lrTestRMSE = lrEvaluator.evaluate(lrTestPredictions)
    println(s"Linear Regression RMSE on 2025 test data = $lrTestRMSE")

    // Predict with Random Forest model on 2025 test data
    val rfTestPredictions = rfModel.transform(testData)
    val rfTestRMSE = rfEvaluator.evaluate(rfTestPredictions)
    println(s"Random Forest RMSE on 2025 test data = $rfTestRMSE")

    // Calculate correlations with air temperature
    val correlations = featureCols.map { feature =>
      val corr = weatherDF.stat.corr(feature, "air_temperature")
      (feature, corr)
    }.sortBy(-_._2)

    println("Correlations with air_temperature:")
    correlations.foreach { case (feature, corr) =>
      println(f"$feature: $corr%.4f")
    }

    // Identify highest correlation
    val highestCorrelation = correlations.maxBy(_._2)
    println(s"The feature with highest correlation to air temperature is: ${highestCorrelation._1} with correlation of ${highestCorrelation._2}")

    // Format the first 10 predictions for reporting
    val lrFormattedPredictions = lrTestPredictions.select(
      $"year",
      $"month",
      $"day",
      $"hour",
      $"air_temperature",
      $"prediction"
    ).orderBy("year", "month", "day", "hour").limit(10)

    val rfFormattedPredictions = rfTestPredictions.select(
      $"year",
      $"month",
      $"day",
      $"hour",
      $"air_temperature",
      $"prediction"
    ).orderBy("year", "month", "day", "hour").limit(10)

    // Generate the analysis report
    val currentDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    val modelPerformanceDiff = Math.abs(lrTestRMSE - rfTestRMSE)
    val betterModel = if (lrTestRMSE < rfTestRMSE) "Linear Regression" else "Random Forest"
    val trainingTimeComparison = if (lrTrainTime < rfTrainTime) "Random Forest was slower to train" else "Linear Regression was slower to train"

    // Collect data for the report
    val lrRows = lrFormattedPredictions.collect()
    val rfRows = rfFormattedPredictions.collect()

    // Build the formatted report string
    val reportBuilder = new StringBuilder()
    reportBuilder.append("=== Weather Prediction Analysis Report ===\n")
    reportBuilder.append(s"Generated: $currentDateTime\n\n")

    reportBuilder.append("1. DATA SUMMARY:\n")
    reportBuilder.append("----------------\n")
    reportBuilder.append(s"- Total records processed: $count\n")
    reportBuilder.append(s"- Training period: 1949-2023 ($trainingCount records)\n")
    reportBuilder.append(s"- Validation year: 2024 ($validationCount records)\n")
    reportBuilder.append(s"- Test year: 2025 ($testCount records)\n\n")

    reportBuilder.append("2. MODEL PERFORMANCE:\n")
    reportBuilder.append("--------------------\n")
    reportBuilder.append("Linear Regression:\n")
    reportBuilder.append(s"- Validation RMSE (2024): $lrValidationRMSE\n")
    reportBuilder.append(s"- Test RMSE (2025): $lrTestRMSE\n")
    reportBuilder.append(f"- Training time: $lrTrainTime%.2f seconds\n\n")

    reportBuilder.append("Random Forest:\n")
    reportBuilder.append(s"- Validation RMSE (2024): $rfValidationRMSE\n")
    reportBuilder.append(s"- Test RMSE (2025): $rfTestRMSE\n")
    reportBuilder.append(f"- Training time: $rfTrainTime%.2f seconds\n\n")

    reportBuilder.append("3. PERFORMANCE COMPARISON:\n")
    reportBuilder.append("-------------------------\n")
    reportBuilder.append(s"$trainingTimeComparison\n")
    reportBuilder.append("but achieved better accuracy\n\n")

    reportBuilder.append("4. FEATURE ANALYSIS:\n")
    reportBuilder.append("-------------------\n")
    reportBuilder.append("Feature Importances (Random Forest):\n")
    featureImportanceMap.foreach { case (feature, importance) =>
      reportBuilder.append(f"  - $feature%-15s $importance%.4f\n")
    }
    reportBuilder.append("\n")

    reportBuilder.append("Temperature Correlations:\n")
    correlations.foreach { case (feature, corr) =>
      reportBuilder.append(f"  - $feature%-15s $corr%.4f\n")
    }
    reportBuilder.append("\n")
    reportBuilder.append(s"Strongest Correlation: ${highestCorrelation._1} (${highestCorrelation._2})\n\n")

    reportBuilder.append("5. OBSERVATIONS:\n")
    reportBuilder.append("---------------\n")
    reportBuilder.append(s"The $betterModel model outperformed ${if (betterModel == "Linear Regression") "Random Forest" else "Linear Regression"}\n\n")

    reportBuilder.append("Key Findings:\n")
    reportBuilder.append(s"1. The most predictive feature was ${featureImportanceMap.head._1}\n")
    reportBuilder.append(s"2. Temperature shows strongest correlation with ${highestCorrelation._1}\n")
    reportBuilder.append(s"3. Model performance difference: $modelPerformanceDiff RMSE\n\n")

    reportBuilder.append("6. SAMPLE PREDICTIONS:\n")
    reportBuilder.append("---------------------\n")
    reportBuilder.append("First 10 test predictions (Linear Regression):\n")
    reportBuilder.append("----------------------------------------------\n")
    lrRows.foreach { row =>
      val year = row.getAs[Int]("year")
      val month = row.getAs[Int]("month")
      val day = row.getAs[Int]("day")
      val hour = row.getAs[Int]("hour")
      val actual = row.getAs[Double]("air_temperature")
      val predicted = row.getAs[Double]("prediction")
      reportBuilder.append(f"$month/$day/$year $hour:00 | Actual: $actual%.1f°C | Predicted: $predicted%.1f°C\n")
    }
    reportBuilder.append("\n\n")

    reportBuilder.append("First 10 test predictions (Random Forest):\n")
    rfRows.foreach { row =>
      val year = row.getAs[Int]("year")
      val month = row.getAs[Int]("month")
      val day = row.getAs[Int]("day")
      val hour = row.getAs[Int]("hour")
      val actual = row.getAs[Double]("air_temperature")
      val predicted = row.getAs[Double]("prediction")
      reportBuilder.append(f"$month/$day/$year $hour:00 | Actual: $actual%.1f°C | Predicted: $predicted%.1f°C\n")
    }

    // Print the final report
    println("\n\n" + reportBuilder.toString())

    // Save the report to a file
    val reportFile = "report.txt"
    import java.io.{File, PrintWriter}
    new PrintWriter(reportFile) { write(reportBuilder.toString()); close() }
    println(s"Report saved to $reportFile")

    // Final timing
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(f"Total execution time: $totalTime%.2f seconds")

    // Stop the Spark session
    spark.stop()
  }
}