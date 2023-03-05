package vigil.core

import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import vigil.Constants
import vigil.model.OddNumber

import java.time.{LocalDate, LocalTime}

/**
 * This class is responsible to handle files action
 * It is used to read and write files
 *
 * @param sparkSession - sparkSession created in Main file
 */
class FileHandler(val sparkSession: SparkSession) {


  val log: Logger = org.apache.log4j.LogManager.getLogger("Vigil")

  private val defaultSchema = StructType(StructField("id", StringType, true) :: StructField("value", StringType, true) :: Nil)

  /**
   * Functions to get all key and values from csv and tsv files using union and cleaning zero values
   *
   * @param input - input path directory
   * @return
   */
  def getAllNumbers(input: String): Dataset[OddNumber] = {

    // read all csv files
    val csvFiles = readFiles(
      input,
      Constants.CSV_FILE_TYPE,
      Constants.CSV_FILE_SEPARATOR
    )

    // read all tsv files
    val tsvFiles = readFiles(
      input,
      Constants.TSV_FILE_TYPE,
      Constants.TSV_FILE_SEPARATOR
    )

    // union csv and tsv files
    filesUnion(csvFiles, tsvFiles)
      .filter("id != 0 and value != 0")
  }

  /**
   * Function responsible to read files based on filePath, fileType and separator
   * It will return a DataFrame based on files found otherwise it will return an empty dataframe
   *
   * @param filePath - files path
   * @param fileType - file type
   * @param sep      - separator parameter
   * @return
   */
  def readFiles(
                 filePath: String,
                 fileType: String,
                 sep: String
               ): sql.DataFrame = {
    try {
      sparkSession.read
        .format("com.databricks.spark.csv")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*." + fileType)
        .option("header", "true")
        .option("sep", sep)
        //.csv(filePath + "/**." + fileType)
        .csv(filePath)
        .na.fill("0")
        .toDF("id", "value")
    } catch {
      case e: AnalysisException =>
        log.warn(e.getMessage())
        sparkSession.createDataFrame(sparkSession.sparkContext
          .emptyRDD[Row], defaultSchema)
    }
  }

  /**
   * Functions used to union csv and tsv dataframes
   * It takes in count if one of them is empty
   * Also if both are empty it will return an empty Dataset
   *
   * @param csv - csv dataframe
   * @param tsv - tsv dataframe
   * @return
   */
  def filesUnion(csv: sql.DataFrame, tsv: sql.DataFrame): Dataset[OddNumber] = {
    import sparkSession.implicits._

    val union = if (csv.isEmpty && tsv.isEmpty) {
      sparkSession.emptyDataset[OddNumber]
    } else if (csv.isEmpty) {
      tsv
    } else if (tsv.isEmpty) {
      csv
    } else {
      csv.union(tsv)
    }

    union.as[OddNumber]
  }

  /**
   * Private function to create a more readable file name
   * This was created in case you intend to run same request more than once
   *
   * @return
   */
  private def createFilename(): String = {
    val time = (LocalDate.now(), LocalTime.now())
    s"/${time._1}-${time._2}"
  }

  /**
   * Function responsible to write request output
   * It will return true if succeed and false if something gone wrong
   * Failure error will be logged
   *
   * @param output - output file path
   * @param ds     - dataset to be saved
   * @return
   */
  def writeFile(output: String, ds: Dataset[Row]): Boolean = {
    try {
      ds
        .write
        .option("header", "true")
        .option("sep", "\t")
        .csv(output + createFilename())
      true
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        false
    }
  }

}
