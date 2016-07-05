package com.jn.spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-21.
 */
object TopN {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("TopN").setMaster("local")
        val sc = new SparkContext(conf)

        val path = "/spark/sparkdata/sort.txt"
        val lines = sc.textFile(path)
        val words = lines.flatMap(line => line.split("\t"))
        val pairs = words.map((_,1)).reduceByKey(_+_)
        val sort = pairs.map{case(key,value) => (value,key)}.sortByKey(true,1)
        val topn = sort.top(3)
        topn.foreach(println)
        sc.stop()
    }
}
/**
 * 3===3
1===3
20===2
5===2
6===2
8===1
55===1
9===1
10===1
 结果

  */