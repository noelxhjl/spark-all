package org.example.test.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    // 建立和Spark框架连接

    //JDBC Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
    val sparkContext = new SparkContext(sparkConf);

    //业务操作
    // 1. 读取文件，获取行数据
    // hello world
    val lines: RDD[String] = sparkContext.textFile(path = "data")


    // 2. 将行数据拆分成一个一个的单词
    // hello world => hello, hello, world, world
    val words :RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => ( word, 1 )
    )
    // (hello, 1)

    //相同的Key的数据，可以对Value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_+_)



    // 5. 结果打印
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //关闭连接
    sparkContext.stop();
  }
}
