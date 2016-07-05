package com.jn.spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-22.
 */
object SecondarySortApp {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("SecondarySortApp").setMaster("local")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("/spark/sparkdata/sort.txt")
        val pairWithSortKey = lines.map(line => (new SecondSortKey(line.split("\t")(0).toInt,line.split("\t")(1).toInt),line))
        val sorted = pairWithSortKey.sortByKey(false)
        val result = sorted.map(sortLine =>sortLine._2)
        result.collect().foreach(println)
    }
}

/**
5	10
5	8
3	9
3	6
3	6
1	55
1	20
1	20
  */