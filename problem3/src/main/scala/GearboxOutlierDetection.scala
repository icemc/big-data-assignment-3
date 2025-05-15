
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object GearboxOutlierDetection {

  def main(args: Array[String]): Unit = {
    // Disable excessive logging
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("GearboxOutlierDetection")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define the expected schema to handle schema inference errors
    val expectedSchema = StructType(Seq(
      StructField("sensor1", DoubleType, nullable = true),
      StructField("sensor2", DoubleType, nullable = true),
      StructField("sensor3", DoubleType, nullable = true)
    ))

    val combinedData = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .schema(expectedSchema)
      .csv("../../input/gearbox/*.csv") // All CSV files in the directory
      .toDF("sensor1", "sensor2", "sensor3")
      .withColumn("runId", regexp_extract(input_file_name(), "Run_(\\d+)\\.csv", 1).cast("int"))

    // Cache the data since we'll be using it multiple times
    combinedData.cache()


    // Print basic statistics
    println(s"Total number of readings: ${combinedData.count()}")
    println("Sample data:")
    combinedData.show(5)

    // Prepare features vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("sensor1", "sensor2", "sensor3"))
      .setOutputCol("features")

    val featureData = assembler.transform(combinedData)

    // Normalize the features
    val featureMeans = featureData.select(
      mean("sensor1").as("mean1"),
      mean("sensor2").as("mean2"),
      mean("sensor3").as("mean3")
    ).first()

    val featureStdDevs = featureData.select(
      stddev("sensor1").as("std1"),
      stddev("sensor2").as("std2"),
      stddev("sensor3").as("std3")
    ).first()

    // User-defined function for normalization
    val normalizeUDF = udf((v1: Double, v2: Double, v3: Double) => {
      val normalizedV1 = if (featureStdDevs.getDouble(0) > 0) (v1 - featureMeans.getDouble(0)) / featureStdDevs.getDouble(0) else 0.0
      val normalizedV2 = if (featureStdDevs.getDouble(1) > 0) (v2 - featureMeans.getDouble(1)) / featureStdDevs.getDouble(1) else 0.0
      val normalizedV3 = if (featureStdDevs.getDouble(2) > 0) (v3 - featureMeans.getDouble(2)) / featureStdDevs.getDouble(2) else 0.0

      Vectors.dense(normalizedV1, normalizedV2, normalizedV3)
    })

    val normalizedData = featureData.withColumn("normalizedFeatures",
      normalizeUDF($"sensor1", $"sensor2", $"sensor3")).cache()

    normalizedData.count() // Force evaluation and caching before looping

    // Function to calculate distance to nearest centroid
    def distToCentroid(point: Vector, centroids: Array[Vector]): Double = {
      centroids.map(centroid => {
        math.sqrt(Vectors.sqdist(point, centroid))
      }).min
    }

    // For each k from 2 to 12, run K-means and find outliers
    for (k <- 2 to 12) {
      println(s"\n========== Running K-means with k = $k ==========")

      // Create K-means model
      val kmeans = new KMeans()
        .setK(k)
        .setSeed(1L)
        .setFeaturesCol("normalizedFeatures")
        .setPredictionCol("cluster")
        .setMaxIter(100)
        .setTol(1.0e-6)

      val model = kmeans.fit(normalizedData)

      // Get the cluster centers
      val centroids = model.clusterCenters
      println(s"Cluster centers for k = $k:")
      centroids.zipWithIndex.foreach { case (center, idx) =>
        println(s"Cluster $idx: $center")
      }

      // Calculate distances to nearest centroid for each point
      val distanceUDF = udf((features: Vector) => distToCentroid(features, centroids))

      val clusteredData = model.transform(normalizedData)

      val withDistances = clusteredData
        .withColumn("distance", distanceUDF($"normalizedFeatures"))

      // Find top-25 outliers (points with largest distance to nearest centroid)
      println(s"\nTop-25 outliers for k = $k:")
      withDistances
        .orderBy(desc("distance"))
        .select("runId", "sensor1", "sensor2", "sensor3", "cluster", "distance")
        .limit(25)
        .show()

      // Calculate average distance to nearest centroid (clustering score)
      val avgDistance = withDistances.agg(avg("distance")).first().getDouble(0)
      println(s"Average distance to nearest centroid for k = $k: $avgDistance")

      // Calculate cluster sizes
      val clusterCounts = withDistances.groupBy("cluster").count().orderBy("cluster")
      println("Cluster sizes:")
      clusterCounts.show()

      // Save sample data for visualization (1% of data)
      val sampleForVisualization = withDistances
        .sample(withReplacement = false, 0.01)
        .select("sensor1", "sensor2", "sensor3", "cluster", "distance")

      sampleForVisualization.coalesce(1).write
        .option("header", "true")
        .csv(s"visualization_k${k}")
    }

    // Stop the SparkSession
    spark.stop()
  }
}