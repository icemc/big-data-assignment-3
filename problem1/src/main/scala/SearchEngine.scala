import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vectors, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}

import java.io._

object SearchEngine {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SearchEngine <modelDir> <comma_separated_queries>")
      System.exit(1)
    }

    val modelDir = args(0)
    val queriesInput = args(1)
    val queryList = queriesInput.split(",").map(_.trim).filter(_.nonEmpty)

    val conf = new SparkConf().setAppName("LSA Search Engine").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // Load US matrix
    val usData = sc.textFile(s"$modelDir/US").map(line => {
      val arr = line.split(",").map(_.toDouble)
      Vectors.dense(arr)
    })
    val US = new RowMatrix(usData)

    // Load V matrix
    val vReader = new ObjectInputStream(new FileInputStream(s"$modelDir/V.ser"))
    val V = vReader.readObject().asInstanceOf[Matrix]
    vReader.close()

    // Load IDF and term maps
    val idTermsReader = new ObjectInputStream(new FileInputStream(s"$modelDir/idTerms.ser"))
    val idTerms = idTermsReader.readObject().asInstanceOf[Map[String, Int]]
    idTermsReader.close()

    val idfsReader = new ObjectInputStream(new FileInputStream(s"$modelDir/idfs.ser"))
    val idfs = idfsReader.readObject().asInstanceOf[Map[String, Double]]
    idfsReader.close()

    val docIdsReader = new ObjectInputStream(new FileInputStream(s"$modelDir/docIds.ser"))
    val docIds = docIdsReader.readObject().asInstanceOf[collection.Map[Long, String]]
    docIdsReader.close()

    // Convert terms to vector
    def termsToQueryVector(terms: Seq[String]): BSparseVector[Double] = {
      val indices = terms.filter(idTerms.contains).map(idTerms).toArray
      val values = terms.filter(idfs.contains).map(idfs).toArray
      new BSparseVector[Double](indices, values, idTerms.size)
    }

    // Perform search
    def search(query: Seq[String], topN: Int = 10): Seq[(String, Double)] = {
      val queryVec = termsToQueryVector(query)
      val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
      val projectedQuery = breezeV.t * queryVec

      val queryMatrix = new org.apache.spark.mllib.linalg.DenseMatrix(projectedQuery.length, 1, projectedQuery.toArray)
      val docScores = US.multiply(queryMatrix)


      docScores.rows.map(_.toArray(0)).zipWithUniqueId()
        .filter { case (_, id) => docIds.contains(id) }
        .map { case (score, id) => (score, docIds(id)) }
        .top(topN)(Ordering.by(_._1))
        .map { case (score, doc) => (doc, score) }
    }

    // Execute search for each query
    for (queryStr <- queryList) {
      val queryTerms = queryStr.toLowerCase.split("\\s+").toSeq
      val results = search(queryTerms)
      println(s"\nTop results for: '$queryStr'")
      results.foreach { case (doc, score) => println(f"$doc: $score%.4f") }
    }

    spark.stop()
  }
}
