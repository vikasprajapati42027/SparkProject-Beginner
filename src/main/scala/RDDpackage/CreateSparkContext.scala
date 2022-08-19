package RDDpackage

import org.apache.spark.{SparkConf, SparkContext}

object CreateSparkContext {
  def main(args: Array[String]): Unit = {

     val sparkConf = new SparkConf()
       sparkConf.setAppName("first sSpark Application")
       sparkConf.setMaster("local")

       val sc=new SparkContext(sparkConf)

    val array=Array(1,2,3,4,5,6,7,8,9)

    val arrayRDD = sc.parallelize(array,2)

    println("Num of elements in RDD : ", arrayRDD.count())
    arrayRDD.foreach(println)

   val file = "/home/sterlite/CSV_file_with_data/iris.csv"
   val fileRDD = sc.textFile(file,5)
    println("Num of rows in FIle "+ fileRDD.count())
    println(fileRDD.first())


  }
}
