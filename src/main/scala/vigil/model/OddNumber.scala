package vigil.model

/**
 * Class used to convert Dataframes in Datasets
 *
 * Datasets are recommended because they use less memory than RDD or Dataframes when cached
 *
 * @param id
 * @param value
 */
case class OddNumber(id: String, value: String)
