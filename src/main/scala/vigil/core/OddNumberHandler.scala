package vigil.core

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Dataset, Row}
import vigil.model.OddNumber

/**
 * This class is responsible to apply business requirements to main Datasets
 */
class OddNumberHandler {

  /**
   * Function receives a dataset and return a filtered dataset
   * Action: filter odd value occurrences
   *
   * @param ds - dataset
   * @return
   */
  def getOddValueNumberOccurrences(ds: Dataset[OddNumber]): Dataset[Row] = {
    ds
      .groupBy(col("id"), col("value"))
      .agg(count("value"))
      .where((count("value") % 2).notEqual(0))
      .drop("count(value)")
  }

}
