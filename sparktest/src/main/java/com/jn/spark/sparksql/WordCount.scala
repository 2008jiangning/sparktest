package com.jn.spark.sparksql

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-20.
 */
object WordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")
        val sc = new SparkContext(conf)

        val path = "/spark/sparkdata/sort.txt"
        val lines = sc.textFile(path)
        val words = lines.flatMap(line => (line.split("\t")))
        val pairs = words.map(word => (word,1))
        val wordCount = pairs.reduceByKey(_ + _)
        .map(pair => (pair._2,pair._1)).sortByKey(false,1).map(pair => (pair._2,pair._1))
        wordCount.collect().foreach(word => println(word._1+"==="+ word._2))
    }
}
