import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.collection.mutable._
import scala.io.Source._

object RunLSA {
  def main(args: Array[String]): Unit = {
    // Command line argument parsing
    val sampleSize = if (args.length > 0) args(0).toDouble else 0.01
    val numTerms = if (args.length > 1) args(1).toInt else 5000
    val k = if (args.length > 2) args(2).toInt else 25
    val dataPath = if (args.length > 3) s"${args(3)}/*/*" else "../../input/wikipedia/articles/*/*"
    val stopwordsPath = if (args.length > 4) args(4) else "../../input/wikipedia/stopwords.txt"

    // Disable excessive logging
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Initialize Spark
    val conf = new SparkConf().setAppName("LSA with NLP")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    println(s"Starting LSA with parameters: sampleSize=$sampleSize, numTerms=$numTerms, k=$k")
    println(s"Reading data from: $dataPath")

    // Start timer
    val startTime = System.currentTimeMillis()

    // Functions to parse Wikipedia articles
    def parseHeader(line: String): Array[String] = {
      try {
        var s = line.substring(line.indexOf("id=\"") + 4)
        val id = s.substring(0, s.indexOf("\""))
        s = s.substring(s.indexOf("url=\"") + 5)
        val url = s.substring(0, s.indexOf("\""))
        s = s.substring(s.indexOf("title=\"") + 7)
        val title = s.substring(0, s.indexOf("\""))
        Array(id, url, title)
      } catch {
        case _: Exception => Array("", "", "")
      }
    }

    def parse(lines: Array[String]): Array[(String, String)] = {
      val docs = ArrayBuffer.empty[(String, String)]
      var title = ""
      var content = ""
      for (line <- lines) {
        try {
          if (line.startsWith("<doc ")) {
            title = parseHeader(line)(2)
            content = ""
          } else if (line.startsWith("</doc>")) {
            if (title.nonEmpty && content.nonEmpty) {
              docs += ((title, content))
            }
          } else {
            content += line + "\n"
          }
        } catch {
          case _: Exception => content = ""
        }
      }
      docs.toArray
    }

    // Read text files
    val textFiles = sc.wholeTextFiles(dataPath).sample(withReplacement = false, sampleSize)
    val numFiles = textFiles.count()
    println(s"Found $numFiles files to process")

    // Parse Wikipedia articles
    val plainText = textFiles.flatMap { case (_, text) => parse(text.split("\n")) }
    val numDocs = plainText.count()
    println(s"Extracted $numDocs documents")

    val bNumDocs = sc.broadcast(numDocs)

    // Helper functions
    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }

    // Load stopwords
    val stopWords = fromFile(stopwordsPath).getLines().toSet
    val bStopWords = sc.broadcast(stopWords)

    // NLP pipeline for lemmatization
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

    // Process text with NLP
    val processedText: RDD[(String, mutable.Seq[String])] =
      plainText.mapPartitions(it => {
        val pipeline = createNLPPipeline()
        it.map {
          case (title, contents) =>
            (title, plainTextToLemmas(contents, pipeline))
        }
      })


    // Calculate term frequencies
    val docTermFreqs = processedText.map {
      case (title, terms) =>
        val termFreqs = terms.foldLeft(new mutable.HashMap[String, Int]()) {
          (map, term) => {
            map += term -> (map.getOrElse(term, 0) + 1)
            map
          }
        }
        (title, termFreqs)
    }.cache()

    docTermFreqs.count() // Force evaluation

    // Calculate document frequencies
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
    val docFreqs = docTermFreqs.flatMap(_._2.keySet).map((_, 1)).reduceByKey(_ + _, 24)

    val ordering = Ordering.by[(String, Int), Int](_._2)
    val topDocFreqs = docFreqs.top(numTerms)(ordering)

    // Calculate inverse document frequencies
    val idfs = topDocFreqs.map {
      case (term, count) =>
        (term, math.log(bNumDocs.value.toDouble / count))
    }.toMap

    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value

    // Create TF-IDF vectors
    val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, _) => bIdTerms.contains(term)
      }.map {
        case (term, _) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.size, termScores)
    }).cache()

    vecs.count() // Force evaluation

    // Compute SVD
    val mat = new RowMatrix(vecs)
    val svd = mat.computeSVD(k, computeU = true)

    println(s"SVD computed with k=$k. Singular values: ${svd.s.toArray.mkString(", ")}")

    // Functions for querying results
    def topTermsInTopConcepts(
                               svd: SingularValueDecomposition[RowMatrix, Matrix],
                               numConcepts: Int, numTerms: Int): mutable.Seq[mutable.Seq[(String, Double)]] = {
      val v = svd.V
      val topTerms = new ArrayBuffer[mutable.Seq[(String, Double)]]()
      val arr = v.toArray
      for (i <- 0 until numConcepts) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
          case (score, id) =>
            (idTerms.find(_._2 == id).getOrElse(("", -1))._1, score)
        }
      }
      topTerms
    }

    def topDocsInTopConcepts(
                              svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int, numDocs: Int): mutable.Seq[mutable.Seq[(String, Double)]] = {
      val u = svd.U
      val topDocs = new ArrayBuffer[mutable.Seq[(String, Double)]]()
      for (i <- 0 until numConcepts) {
        val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
        topDocs += docWeights.top(numDocs).map {
          case (score, id) => (docIds.getOrElse(id, "unknown"), score)
        }
      }
      topDocs
    }

    // Helper function for keyword searches
    def termsToQueryVector(
                            terms: immutable.Seq[String],
                            idTerms: immutable.Map[String, Int],
                            idfs: immutable.Map[String, Double]): BSparseVector[Double] = {
      val indices = terms.filter(idTerms.contains).map(idTerms(_)).toArray
      val values = terms.filter(idfs.contains).map(idfs(_)).toArray
      new BSparseVector[Double](indices, values, idTerms.size)
    }

    def topDocsForTermQuery(
                             US: RowMatrix,
                             V: Matrix,
                             idTerms: immutable.Map[String, Int],
                             idfs: immutable.Map[String, Double],
                             docIds: collection.Map[Long, String],
                             query: immutable.Seq[String],
                             numResults: Int = 10): mutable.Seq[(String, Double)] = {
      val queryVec = termsToQueryVector(query, idTerms, idfs)
      val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
      val termRowArr = (breezeV.t * queryVec).toArray
      val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
      val docScores = US.multiply(termRowVec)
      val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()
      allDocWeights.top(numResults).map {
        case (score, id) => (docIds.getOrElse(id, "unknown"), score)
      }
    }

    // Display top terms and docs in top concepts
    val topConceptTerms = topTermsInTopConcepts(svd, k, 25)
    val topConceptDocs = topDocsInTopConcepts(svd, k, 25)

    println("Top terms and documents in top concepts:")
    for (i <- 0 until math.min(k, 25)) {
      println(s"Concept $i terms: ${topConceptTerms(i).map(_._1).mkString(", ")}")
      println(s"Concept $i docs: ${topConceptDocs(i).map(_._1).mkString(", ")}")
      println()
    }

    // Helper function for keyword searches
    def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
      val sArr = diag.toArray
      new RowMatrix(mat.rows.map { vec =>
        val vecArr = vec.toArray
        val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
        Vectors.dense(newArr)
      })
    }

    // Prepare for search functionality (test)
    val US = multiplyByDiagonalRowMatrix(svd.U, svd.s)

    // Example search
    val testQuery = List("serious", "incident")
    val searchResults = topDocsForTermQuery(US, svd.V, idTerms, idfs, docIds, testQuery)

    println(s"Search results for query '${testQuery.mkString(" ")}': ")
    searchResults.foreach { case (doc, score) => println(f"$doc: $score%.4f") }

    // Save SVD components for use in search engine
    import java.io._

    // Paths to save model
    val modelDir = s"RunLSA/model_terms${numTerms}_k${k}"
    new File(modelDir).mkdirs()

    // Save US matrix as row vectors
    US.rows.map(_.toArray.mkString(",")).saveAsTextFile(s"$modelDir/US")

    // Save V matrix
    val vWriter = new ObjectOutputStream(new FileOutputStream(s"$modelDir/V.ser"))
    vWriter.writeObject(svd.V)
    vWriter.close()

    // Save term ID map
    val termMapWriter = new ObjectOutputStream(new FileOutputStream(s"$modelDir/idTerms.ser"))
    termMapWriter.writeObject(idTerms)
    termMapWriter.close()

    // Save IDF map
    val idfWriter = new ObjectOutputStream(new FileOutputStream(s"$modelDir/idfs.ser"))
    idfWriter.writeObject(idfs)
    idfWriter.close()

    // Save docIds map
    val docIdsWriter = new ObjectOutputStream(new FileOutputStream(s"$modelDir/docIds.ser"))
    docIdsWriter.writeObject(docIds)
    docIdsWriter.close()

    spark.stop()
  }
}