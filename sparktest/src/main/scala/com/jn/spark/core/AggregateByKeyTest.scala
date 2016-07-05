package com.jn.spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-29.
 */
object AggregateByKeyTest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("AggregateByKeyTest").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val data = sc.parallelize(List((1,3),(1,2),(1,4),(2,1),(2,4)))
        data.aggregateByKey(1)(seq,comb).collect().foreach(println)
    }
    def seq(a:Int,b: Int): Int ={
        println("seq="+a+":==="+b)
        math.max(a,b)
    }
    def comb(a: Int,b: Int): Int = {
        println("comb"+ a + ":==" + b)
        a+b
    }
}
