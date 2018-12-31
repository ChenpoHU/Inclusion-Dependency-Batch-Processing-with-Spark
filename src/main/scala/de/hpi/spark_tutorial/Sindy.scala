package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.backuity.clist
import org.backuity.clist.opt


object Sindy extends clist.CliMain[Unit](
      name = "Sindy",
      description = "discover inclusion dependencies with spark"
    ) {

  var path: String  = opt[String](description = "path to the folder containing the CSV data", default = "./TPCH")
  var cores: Int = opt[Int](description = "number of cores", default = 4)

  override def run: Unit = {


    val dataDir = this.path
    val datasetNames = Seq("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
    val inputs = datasetNames.map(name => s"$dataDir/tpch_$name.csv")


    val sparkSess: SparkSession = SparkSession
      .builder()
      .appName("Sindy")
      .master(s"local[$cores]") // local, with <cores> worker cores
      .getOrCreate()

    // configs
    sparkSess.conf.set("spark.sql.shuffle.partitions", "16")
    import sparkSess.implicits._

    // get the inputs
    val datasets: Seq[DataFrame] = inputs.map(name => {
      sparkSess.read
        //      .option("inferSchema", "true") // ignore data types (we want to have strings)
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(name)
    })

    val tuplesPerFile: Seq[Dataset[(String, String)]] = datasets.map(df => {
      val cols = df.columns
      df.map    ( row => row.toSeq.map(String.valueOf) )
        .flatMap( row => row.zip(cols)                 )
    })

    val allTuples: Dataset[(String, String)] = tuplesPerFile.reduce { (acc, ds2) => acc.union(ds2) }


    val attribute_Sets = allTuples
      .map(tuple => tuple._1 -> Set(tuple._2))
      .groupByKey(tuple => tuple._1)
      .mapValues { case (_, columnSet) => columnSet }
      .reduceGroups((acc, columnSet) => acc ++ columnSet)
      .map { case (_, columnSet) => columnSet }
      .distinct()

    val inclusionLists = attribute_Sets.flatMap(attributeSet => {
      attributeSet.map(value => value -> (attributeSet - value))
    })

    val tmp = inclusionLists
      .groupByKey(tuple => tuple._1)
      .mapValues { case (_, otherColumnSet) => otherColumnSet }
      .reduceGroups((acc, columnSet) => acc.intersect(columnSet))
      .filter(tuple => tuple._2.nonEmpty)

    tmp.collect()
      .sortBy(tuple => tuple._1)
      .foreach { case (dependent, references) => println(s"$dependent < ${references.mkString(", ")}") }
  }
}
