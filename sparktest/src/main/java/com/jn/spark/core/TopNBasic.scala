package com.jn.spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-22.
 * 分组取出topn
 * [root@hadoop1 sparkdata]# cat topNGroup.txt
Spark 100
Hadoop 65
Spark 99
Hadoop 61
Spark 195
Hadoop 60
Spark 98
Hadoop 69
Spark 91
Hadoop 64
Spark 89
Hadoop 98
Spark 88
Hadoop 99
Spark 68
Hadoop 60
Spark 79
Hadoop 97
Spark 69
Hadoop 96

 */
object TopNBasic {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("TopNBasic").setMaster("local")
        val sc = new SparkContext(conf)

        val path = "/spark/sparkdata/topNGroup.txt"
        val lines = sc.textFile(path)
        val pairs = lines.map(line => (line.split(" ")(0),line.split(" ")(1).toInt))
        val grouppairs = pairs.groupByKey()
        val top5Pairs = grouppairs.map(pair =>(pair._1,getTop5Elements(pair._2))).sortByKey()
        top5Pairs.collect().foreach(println)

    }
    def getTop5Elements(value: Iterable[Int]):Iterable[Int]={
        import scala.collection.mutable.ArrayBuffer
        val groupValue = value.toArray
        val top5 = groupValue.sorted(Ordering.Int.reverse).take(5)
        top5.toArray.toIterable
    }
}
/**
(Hadoop,WrappedArray(99, 98, 97, 96, 69))
(Spark,WrappedArray(195, 100, 99, 98, 91))
  */