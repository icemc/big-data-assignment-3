import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable._
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

object WikipediaMoviePlotLSA {
  def main(args: Array[String]): Unit = {

    val dataPath = if (args.length > 0) args(0) else "../../input/movie_plots/wiki_movie_plots_deduped.csv"
    val stopwordsPath = if (args.length > 1) args(1) else "../../input/stopwords.txt"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Movie Plots LSA")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define schema for the movie plots CSV
    val movieSchema = StructType(Array(
      StructField("Release Year", IntegerType, nullable = true),
      StructField("Title", StringType, nullable = true),
      StructField("Origin/Ethnicity", StringType, nullable = true),
      StructField("Director", StringType, nullable = true),
      StructField("Cast", StringType, nullable = true),
      StructField("Genre", StringType, nullable = true),
      StructField("Wiki Page", StringType, nullable = true),
      StructField("Plot", StringType, nullable = true)
    ))

    // Read the CSV file with the defined schema
    val moviePlotsDF = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .schema(movieSchema)
      .csv(dataPath)
      .select("Title", "Genre", "Plot")
      // .filter($"Plot".isNotNull) // Filter out empty or very short plots

    // Cache the DataFrame for better performance
    moviePlotsDF.cache()
    val numDocs = moviePlotsDF.count()
    println(s"Number of movie plots: $numDocs")

    // ------------------------ NLP Processing Functions ----------------------

    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }

    // Load stop words
    val stopWords = scala.io.Source.fromFile(stopwordsPath).getLines().toSet
    val bStopWords = spark.sparkContext.broadcast(stopWords)

    def createNLPPipeline(): StanfordCoreNLP = {
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      new StanfordCoreNLP(props)
    }

    def plainTextToLemmas(text: String, pipeline: StanfordCoreNLP): mutable.Seq[String] = {
      val doc = new Annotation(text)
      pipeline.annotate(doc)
      val lemmas = new ArrayBuffer[String]()
      val sentences = doc.get(classOf[SentencesAnnotation]).asScala.toList
      for {
        sentence <- sentences
        token <- sentence.get(classOf[TokensAnnotation]).asScala
      } {
        val lemma = token.get(classOf[LemmaAnnotation]).toLowerCase
        if (lemma.length > 2 && !bStopWords.value.contains(lemma)
          && isOnlyLetters(lemma)) {
          lemmas += lemma
        }
      }
      lemmas
    }

    // ------------------------ Feature Extraction ----------------------

    // Define UDF for feature extraction using NLP
    val extractFeaturesUDF = udf { (text: String) =>
      val pipeline = createNLPPipeline()
      plainTextToLemmas(text, pipeline)
    }

    // Add features column to the DataFrame
    val moviePlotsWithFeaturesDF = moviePlotsDF.withColumn("features", extractFeaturesUDF($"Plot"))

    // Cache the transformed DataFrame
    moviePlotsWithFeaturesDF.cache()
    println("Sample of DataFrame with features:")
    moviePlotsWithFeaturesDF.select("Title", "Genre", "features").show(5, truncate = false)

    // ------------------------ Term Frequency Analysis ----------------------

    val bNumDocs = spark.sparkContext.broadcast(numDocs)

    // Convert DataFrame to RDD for term frequency analysis
    val docTermFreqs = moviePlotsWithFeaturesDF.rdd.map { row =>
      val title = row.getAs[String]("Title")
      val genre = row.getAs[String]("Genre")
      val terms = row.getAs[mutable.Seq[String]]("features")

      val termFreqs = terms.foldLeft(new mutable.HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      (title, genre, termFreqs)
    }

    // Cache the RDD
    docTermFreqs.cache()
    val docTermCount = docTermFreqs.count()
    println(s"Number of documents with term frequencies: $docTermCount")

    // Create a mapping from document titles to unique IDs
    val docTitleGenre = docTermFreqs.map(x => (x._1, x._2)).collectAsMap()
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    // Calculate document frequencies for each term
    val docFreqs = docTermFreqs.flatMap(_._3.keySet).map((_, 1)).reduceByKey(_ + _)

    // Parameters for LSA
    val numTerms = 5000 // Number of frequent terms to extract
    val k = 25 // Number of latent dimensions for SVD

    // Get the top frequent terms
    val ordering = Ordering.by[(String, Int), Int](_._2)
    val topDocFreqs = docFreqs.top(numTerms)(ordering)

    // Calculate inverse document frequency (IDF) for each term
    val idfs = topDocFreqs.map {
      case (term, count) =>
        (term, math.log(bNumDocs.value.toDouble / count))
    }.toMap

    // Create term-to-ID and ID-to-term mappings
    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    // Broadcast these mappings
    val bIdfs = spark.sparkContext.broadcast(idfs)
    val bIdTerms = spark.sparkContext.broadcast(idTerms)

    // ------------------------ SVD Computation ----------------------

    import org.apache.spark.mllib.linalg.{Vectors, Matrix, SingularValueDecomposition}
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    // Create term vectors for each document
    val vecs = docTermFreqs.map { case (title, genre, termFreqs) =>
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bIdTerms.value.contains(term)
      }.map {
        case (term, freq) => (bIdTerms.value(term), bIdfs.value(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.value.size, termScores)
    }

    // Cache the vectors
    vecs.cache()
    val numVecs = vecs.count()
    println(s"Number of term vectors: $numVecs")

    // Create a RowMatrix and compute SVD
    val mat = new RowMatrix(vecs)
    println("Computing SVD...")
    val svd = mat.computeSVD(k, computeU = true)
    println("SVD computation complete.")

    // ------------------------ Analysis Functions ----------------------

    // Function to get top terms for each latent concept
    def topTermsInTopConcepts(
                               svd: SingularValueDecomposition[RowMatrix, Matrix],
                               numConcepts: Int, numTerms: Int): Seq[Seq[(String, Double)]] = {
      val v = svd.V
      val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
      val arr = v.toArray
      for (i <- 0 until numConcepts) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
          case (score, id) =>
            (bIdTerms.value.find(_._2 == id).getOrElse(("", -1))._1, score)
        }
      }
      topTerms
    }

    // Extended function to get top documents with genres for each latent concept
    def topDocsInTopConcepts(
                              svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int, numDocs: Int): Seq[Seq[(String, String, Double)]] = {
      val u = svd.U
      val topDocs = new ArrayBuffer[Seq[(String, String, Double)]]()
      for (i <- 0 until numConcepts) {
        val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
        topDocs += docWeights.top(numDocs).map {
          case (score, id) => {
            val title = docIds(id)
            val genre = docTitleGenre.getOrElse(title, "Unknown")
            (title, genre, score)
          }
        }
      }
      topDocs
    }

    // Function to get top 5 genres for each document
    def getTop5Genres(docs: immutable.Seq[(String, String, Double)]): immutable.Seq[(String, String, immutable.Seq[String], Double)] = {
      docs.map { case (title, genre, score) =>
        val genreList = genre.split(",").map(_.trim).take(5)
        (title, genre, genreList.toList, score)
      }
    }

    // ------------------------ Display Results ----------------------

    // Get top 25 terms for each of the top 25 concepts
    val topConceptTerms = topTermsInTopConcepts(svd, 25, 25)

    // Get top 25 documents for each of the top 25 concepts
    val topConceptDocs = topDocsInTopConcepts(svd, 25, 25)

    // Display results
    println("\n===== Top Terms in Top Concepts =====")
    topConceptTerms.zipWithIndex.foreach { case (terms, idx) =>
      println(s"Concept $idx: ${terms.map(_._1).mkString(", ")}")
    }

    println("\n===== Top Documents with Genres in Top Concepts =====")
    topConceptDocs.zipWithIndex.foreach { case (docs, idx) =>
      println(s"Concept $idx:")
      getTop5Genres(docs.toList).foreach { case (title, fullGenre, topGenres, score) =>
        println(f"  $title (Score: $score%.4f) - Genres: ${topGenres.mkString(", ")}")
      }
    }

    // ------------------------ Search Engine Implementation ----------------------

    import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
    import org.apache.spark.mllib.linalg.{Matrices, Vector => MLLibVector}

    // Function to convert query terms to a vector
    def termsToQueryVector(
                            terms: immutable.Seq[String],
                            idTerms: immutable.Map[String, Int],
                            idfs: immutable.Map[String, Double]): BSparseVector[Double] = {

      val indices = terms.filter(idTerms.contains).map(idTerms(_)).toArray
      val values = terms.filter(idTerms.contains).map(idfs(_)).toArray

      new BSparseVector[Double](indices, values, idTerms.size)
    }

    // Function to find top documents for a query
    def topDocsForTermQuery(
                             US: RowMatrix,
                             V: Matrix,
                             query: BSparseVector[Double]): Seq[(String, String, Double)] = {

      val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
      val termRowArr = (breezeV.t * query).toArray
      val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

      val docScores = US.multiply(termRowVec)
      val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()

      allDocWeights.top(10).map { case (score, id) =>
        val title = docIds(id)
        val genre = docTitleGenre.getOrElse(title, "Unknown")
        (title, genre, score)
      }
    }

    // Helper function to multiply matrices
    def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
      val sArr = diag.toArray
      new RowMatrix(mat.rows.map { vec =>
        val vecArr = vec.toArray
        val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
        Vectors.dense(newArr)
      })
    }

    // Precompute US matrix for queries
    val US = multiplyByDiagonalRowMatrix(svd.U, svd.s)

    // Function to run keyword queries
    def runKeywordQuery(keywords: immutable.Seq[String]): Unit = {
      println(s"\n===== Query: ${keywords.mkString(" ")} =====")
      val queryVec = termsToQueryVector(keywords, bIdTerms.value, bIdfs.value)
      val results = topDocsForTermQuery(US, svd.V, queryVec)

      results.foreach { case (title, genre, score) =>
        println(f"Title: $title (Score: $score%.4f , Genre: $genre)")
      }
    }

    // Run sample queries
    val queries = List(
      List("love", "romance"),
      List("war", "battle"),
      List("comedy", "funny"),
      List("murder", "mystery"),
      List("space", "alien"),
      List("family", "drama"),
      List("horror", "scary"),
      List("adventure", "journey"),
      List("robot", "future"),
      List("western", "cowboy")
    )

    queries.foreach(runKeywordQuery)

    // Clean up
    spark.stop()
  }
}