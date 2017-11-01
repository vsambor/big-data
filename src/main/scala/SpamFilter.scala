import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SpamFilter {

  val USELESS_CHARS = Set[String](
    ".", ":", ",", " ", "/", "\\", "-", "'", "(", ")", "@"
  )

  /**
    * Returns the probability of words in all the files from the given directory.
    *
    * Splits each file words from provided directory and filters out non informative words.
    * Creates a collection of distinct word -> probability; which is returned along with the total
    * number of files in the directory.
    *
    * @param sc {SparkContext} - spark context object which contains the main functions.
    *
    * @param filesDir {String} - the folder directory which contains the files to be analyzed.
    *
    * @return {RDD[(String, Double)], Long} - two variables; an RDD with a collection of words and their probability,
    *         and second variable is the number of files inside the provided directory.
    */
  def probaWordDir(sc: SparkContext)(filesDir: String): (RDD[(String, Double)], Long) = {
    // Reads all text files the filesDir directory.
    val files = sc.wholeTextFiles(filesDir)

    // Number of files.
    val filesNumber = files.count()

    // Splits into a set of unique words. (on non-alphanumeric character)
    val words = files.mapValues(_.split("\\W+")

      // Filters only words.
      .filter(_.matches("[a-zA-Z]+"))

      // Distinct and lowercase to avoid word duplications.
      .distinct.map(word => word.toLowerCase()))

    // Removes useless words.
    val filterWords = words.mapValues(_.filter(word => !USELESS_CHARS(word)))

    // Maps a word to a list of files where this word is included.
    val wordsByFiles = filterWords
      .flatMap(t => t._2.map(w => (w, List(t._1))))
      .reduceByKey(_ ::: _)

    // Counts the number of files in which each word has occurred and saves inside a map (word -> occurrence).
    val wordOccurrencies = wordsByFiles.mapValues(_.length)

    // Calculates the world probability.
    val wordsProbability = wordOccurrencies.mapValues(_ / filesNumber.toDouble)

    // Returns the words probability and he total number of files in the directory.
    (wordsProbability, filesNumber)
  }

  /**
    * Computes the mutual information factor for each word.
    *
    * @param probaWC {RDD[(String, Double)]} - a collection of words associated with their probabilities from
    *                one specific directory.
    *
    * @param probaW {RDD[(String, Double)]} - a collection of words associated with their probabilities from
    *                mixed directories.
    *
    * @param probaC {Double} - represents the probability of having a spam or ham email.
    *
    * @param probaDefault {Double} - the default value when a probability is missing.
    *
    * @return {RDD[(String, Double)]} - the mutual information factor for each word.
    */
  def computeMutualInformationFactor(probaWC: RDD[(String, Double)], probaW: RDD[(String, Double)], probaC: Double,
                                     probaDefault: Double): RDD[(String, Double)] = {
    probaW.leftOuterJoin(probaWC).mapValues {
      case (probaX, Some(probaXY)) => probaXY * math.log(probaXY / (probaX * probaC))
      case (probaX, None) => probaDefault * math.log(probaDefault / (probaX * probaC))
    }
  }

  /**
    * The application entry point.
    *
    * @param args {Array[String] - command line arguments.
    */
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("Spam Filter")
    val sc = new SparkContext(config)

    // Computes the probability for ham directory words.
    val (hamWordsProbability, nbFilesHam) = probaWordDir(sc)("/tmp/ling-spam/ham/")

    // Computes the probability for spam directory words.
    val (spamWordsProbability, nbFilesSpam) = probaWordDir(sc)("/tmp/ling-spam/spam/")

    // To have the probability (occurrence, class), we have to first calculate the probability for Ham and Spam
    // Ham or Spam Probability = number of ham or spam files divided by the number of total files.
    val nbFiles = (nbFilesHam + nbFilesSpam).toDouble
    val spamProba = nbFilesSpam / nbFiles
    val hamProba = nbFilesHam / nbFiles

    // We compute the probability probability for each word.
    // There are two values of class : ham and spam and two values of occurs : true or false
    // So we will obtain 4 RDDs : (true, ham),(false, ham), (true, spam) and (false, spam)
    // (true, ham) : hamWordsProbability * hamProba
    val trueHam = hamWordsProbability.map(x => (x._1, x._2 * hamProba))

    // (false, ham) = (1 - hamWordsProbability) * hamProba
    val falseHam = hamWordsProbability.map(x => (x._1, (1.0 - x._2) * hamProba))

    // (true, spam) = spamWordsProbability * spamProba
    val trueSpam = spamWordsProbability.map(x => (x._1, x._2 * spamProba))

    // (false, spam) = (1 - spamWordsProbability) * spamProba
    val falseSpam = spamWordsProbability.map(x => (x._1, (1.0 - x._2) * spamProba))

    // Default value for words who occurs in only one class (Ham or Spam).
    val default = 0.2 / nbFiles

    // Gets all the words which are true in ham and spam to detect if a word occurs in only one class.
    val words = trueHam.fullOuterJoin(trueSpam)

    // We apply de default value for word in only one class
    val defaultWords = words.mapValues {
      case (trueAndHam, trueAndSpam) => (trueAndHam.getOrElse(default), trueAndSpam.getOrElse(default))
    }

    // We also need the word probability from all classes
    // (the probability that the word occurs when its and ham + the probability the word occurs when it's spam)
    val wordPresent = defaultWords.mapValues {
      case (trueAndHam, trueAndSpam) => trueAndHam + trueAndSpam
    }
    val wordAbsent = wordPresent.mapValues(1.0 - _)

    // Calculates the mutual information for every possible case: (true, ham), (true, spam), (false, ham), (false, spam)
    val mutualInformationList = List(
      computeMutualInformationFactor(trueHam, wordPresent, hamProba, default),
      computeMutualInformationFactor(trueSpam, wordPresent, spamProba, default),
      computeMutualInformationFactor(falseHam, wordAbsent, hamProba, default),
      computeMutualInformationFactor(falseSpam, wordAbsent, spamProba, default)
    )

    // To have the mutual information of each word, it should be summed up with the previously one.
    val mutualInformation = mutualInformationList.reduce {
      (x1, x2) => x1.join(x2).mapValues {
        case (x, y) => x + y
      }
    }

    // Gets the 10 top words.
    val topWords = mutualInformation.sortBy(-_._2)

    // Prints the top 10 words.
    topWords.take(10).foreach(println)

    // Saves on HDFS in "tmp/topWords.txt".
    topWords.saveAsTextFile("/tmp/topWords.txt")
  }

}





