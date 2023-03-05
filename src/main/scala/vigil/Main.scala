package vigil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import vigil.core.{FileHandler, OddNumberHandler}

/**
 * Main class responsible to execute spark batch
 */
object Main extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val log = org.apache.log4j.LogManager.getLogger("Vigil")

  // arguments validation
  if (args.isEmpty || args.length != 3) {
    throw new IllegalArgumentException("Incorrect number of arguments")
  }

  private val (input, output, profile) = (args(0), args(1), args(2))

  // spark session
  val sparkSession = SparkSession.builder
    .appName("VigilTest")
    .master("local[*]")
    .getOrCreate()

  // hadoop configuration
  sparkSession.sparkContext.hadoopConfiguration.set(
    "fs.s3a.aws.credentials.provider",
    classOf[com.amazonaws.auth.DefaultAWSCredentialsProviderChain].getName
  )
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.profile", profile)

  private val fileHandler = new FileHandler(sparkSession)

  // read all csv and tsv files
  private val allOddNumbers = fileHandler.getAllNumbers(input).cache()

  private val oddNumberHandler = new OddNumberHandler

  // find odd value numbers occurrences
  private val oddNumbers = oddNumberHandler.getOddValueNumberOccurrences(allOddNumbers)

  // write output process
  if (fileHandler.writeFile(output, oddNumbers)) {
    log.info("file created and saved in S3 bucket")
  } else {
    log.error("failed to save file in S3 bucket")
  }

  // spark stop
  sparkSession.stop
}