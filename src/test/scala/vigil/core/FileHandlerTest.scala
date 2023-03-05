package vigil.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import vigil.Constants

class FileHandlerTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var sparkSession: SparkSession = _

  val prefix_dir = "src/test/resources/files/"

  override def beforeAll(): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.master", "local")

    sparkSession = SparkSession.builder
      .config(sparkConfig)
      .getOrCreate()
  }

  test("read csv files") {

    val fileHandler = new FileHandler(sparkSession)

    val csvFiles = fileHandler.readFiles(
      prefix_dir + "csv/",
      Constants.CSV_FILE_TYPE,
      Constants.CSV_FILE_SEPARATOR
    )

    assert(csvFiles.count() == 15)
  }

  test("read csv files from empty folder") {

    val fileHandler = new FileHandler(sparkSession)

    val csvFiles = fileHandler.readFiles(
      prefix_dir + "csv/empty",
      Constants.CSV_FILE_TYPE,
      Constants.CSV_FILE_SEPARATOR
    )

    assert(csvFiles.isEmpty)
  }

  test("read csv files with empty keys and values") {

    val fileHandler = new FileHandler(sparkSession)

    val csvFiles = fileHandler.readFiles(
      prefix_dir + "csv/zeros",
      Constants.CSV_FILE_TYPE,
      Constants.CSV_FILE_SEPARATOR
    )

    assert(csvFiles.count() == 10)
  }

  test("read tsv files") {

    val fileHandler = new FileHandler(sparkSession)

    val tsvFiles = fileHandler.readFiles(
      prefix_dir + "tsv/",
      Constants.TSV_FILE_TYPE,
      Constants.TSV_FILE_SEPARATOR
    )

    assert(tsvFiles.count() == 9)
  }

  test("read tsv files from empty folder") {

    val fileHandler = new FileHandler(sparkSession)

    val tsvFiles = fileHandler.readFiles(
      prefix_dir + "tsv/empty",
      Constants.TSV_FILE_TYPE,
      Constants.TSV_FILE_SEPARATOR
    )

    assert(tsvFiles.isEmpty)
  }

  test("read tsv files with empty keys and values") {

    val fileHandler = new FileHandler(sparkSession)

    val tsvFiles = fileHandler.readFiles(
      prefix_dir + "tsv/zeros",
      Constants.TSV_FILE_TYPE,
      Constants.TSV_FILE_SEPARATOR
    )

    assert(tsvFiles.count() == 6)
  }

  test("read all csv and tsv files") {

    val fileHandler = new FileHandler(sparkSession)

    val allNumbers = fileHandler.getAllNumbers(prefix_dir)

    assert(allNumbers.count() == 19)
  }
}
