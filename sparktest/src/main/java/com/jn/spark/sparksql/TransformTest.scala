package com.jn.spark.sparksql

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-22.
 */
object TransformTest {
    def main(args: Array[String]) {
        val sc = getSparkContext()
//        mapTransformationTest(sc)
//        filterTransformationTest(sc)
//        flatMapTransformationTest(sc)
//        groupByKeyTransformationTest(sc)
//        reduceByKeyTransformationTest(sc)
//        joinTransformationTest(sc)
        cogroupTransformationTest(sc)
        sc.stop()

    }
    def getSparkContext()={
        val conf = new SparkConf().setAppName("TransformationTest").setMaster("local")
        val sc = new SparkContext(conf)
        sc
    }
    def mapTransformationTest(sc: SparkContext): Unit ={
        val nums = sc.parallelize(1 to 10)
        val maps = nums.map{item => item*2}
        maps.collect.foreach(println)
    }
    def filterTransformationTest(sc: SparkContext): Unit ={
        val nums = sc.parallelize(1 to 10)
        val filters = nums.filter{item => item%3 ==0}
        filters.collect.foreach(println)
    }

    def flatMapTransformationTest(sc:SparkContext): Unit ={
        val bigData = Array("spark data","hadoop data","flink data")
        val bigDataString = sc.parallelize(bigData)
        val words = bigDataString.flatMap{item => item.split(" ")}
        words.collect().foreach(println)
    }

    def groupByKeyTransformationTest(sc: SparkContext): Unit ={
        val data = Array(Tuple2(100,"spark"),Tuple2(90,"hadoop"),Tuple2(80,"flink"),Tuple2(80,"hbase"))
        val dataRdd = sc.parallelize(data)
        val group = dataRdd.groupByKey()
        group.collect().foreach(println)
    }

    def reduceByKeyTransformationTest(sc: SparkContext): Unit ={
        val data = Array("spark data","hadoop data","hbase good")
        val lines = sc.parallelize(data)
        val words = lines.flatMap{item => item.split(" ")}
        val pairs = words.map(word => (word,1))
        val wordCount = pairs.reduceByKey(_ + _)
        wordCount.collect().foreach(wordNumPair=>println(wordNumPair._1+"=="+wordNumPair._2))
    }

    def joinTransformationTest(sc: SparkContext): Unit ={
        val studentNames = Array(Tuple2(1,"hadoop"),Tuple2(2,"zookeeper"),Tuple2(3,"spark"))
//        val studentScores = Array(Tuple2(1,100),Tuple2(2,90),Tuple2(3,80))
        val studentScores = Array(Tuple2(1,100),Tuple2(2,90))//相当与内连接，没有的数据，就没有了。
        val names = sc.parallelize(studentNames)
        val scores = sc.parallelize(studentScores)
        val join = names.join(scores)//通过key进行join
        join.collect().foreach(println)
    }

    def cogroupTransformationTest(sc: SparkContext): Unit ={
        val studentsName = Array(Tuple2(1,"hadoop"),Tuple2(2,"hbase"),Tuple2(3,"spark"))
        val studentsScore = Array(Tuple2(1,100),Tuple2(2,90),Tuple2(3,80),Tuple2(4,70),Tuple2(1,90),Tuple2(2,800),Tuple2(3,70),Tuple2(4,60))
        val names = sc.parallelize(studentsName)
        val scores = sc.parallelize(studentsScore)
        val cogroup = names.cogroup(scores)//基于key进行join然后进行分组
        cogroup.collect().foreach(println)
    }

    /**
     * (4,(CompactBuffer(),CompactBuffer(70, 60)))
(1,(CompactBuffer(hadoop),CompactBuffer(100, 90)))
(3,(CompactBuffer(spark),CompactBuffer(80, 70)))
(2,(CompactBuffer(hbase),CompactBuffer(90, 800)))
     */

}
