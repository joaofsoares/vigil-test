package vigil.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OddNumberHandlerTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

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

  test("read files and get odd occurrences") {

    val fileHandler = new FileHandler(sparkSession)

    val allNumbers = fileHandler.getAllNumbers(prefix_dir)

    val oddNumberHandler = new OddNumberHandler()

    val resultDataset = oddNumberHandler.getOddValueNumberOccurrences(allNumbers)

    assert(resultDataset.count() == 1)
  }
}
