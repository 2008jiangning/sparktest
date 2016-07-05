package com.jn.spark.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.functions._
/**
 * 使用Spark SQL中的内置函数对数据进行分析，Spark SQL API不同的是，DataFrame中的内置函数操作的结果是返回一个Column对象，而
 * DataFrame天生就是"A distributed collection of data organized into named columns.",这就为数据的复杂分析建立了坚实的基础
 * 并提供了极大的方便性，例如说，我们在操作DataFrame的方法中可以随时调用内置函数进行业务需要的处理，这之于我们构建附件的业务逻辑而言是可以
 * 极大的减少不必须的时间消耗（基于上就是实际模型的映射），让我们聚焦在数据分析上，这对于提高工程师的生产力而言是非常有价值的
 * Spark 1.5.x开始提供了大量的内置函数，例如agg：
 * def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
 *  groupBy().agg(aggExpr, aggExprs : _*)
 *}
 * 还有max、mean、min、sum、avg、explode、size、sort_array、day、to_date、abs、acros、asin、atan
 * 总体上而言内置函数包含了五大基本类型：
 * 1，聚合函数，例如countDistinct、sumDistinct等；
 * 2，集合函数，例如sort_array、explode等
 * 3，日期、时间函数，例如hour、quarter、next_day
 * 4,数学函数，例如asin、atan、sqrt、tan、round等；
 * 5，开窗函数，例如rowNumber等
 * 6，字符串函数，concat、format_number、rexexp_extract
 * 7,其它函数，isNaN、sha、randn、callUDF
 */
object SparkSQLAgg {
    def main(args: Array[String]) {
        /**
         * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
         * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
         * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
         * 只有1G的内存）的初学者       *
         */
        val conf = new SparkConf().setMaster("local").setAppName("SparkSQLAgg")
        /**
         * 第2步：创建SparkContext对象
         * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
         * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
         * 同时还会负责Spark程序往Master注册程序等
         * SparkContext是整个Spark应用程序中最为至关重要的一个对象
         */
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

//        要是用SparkSQL 的内置函数，就一定导入SQLContext下的隐式转换
        import sqlContext.implicits._
        /**
         * 第三步：模拟电商访问的数据，实际情况会比模拟数据复杂很多，最后生成RDD
         */
        val userData = Array(
            "2016-3-27,001,http://spark.apache.org/,1000",
            "2016-3-27,001,http://hadoop.apache.org/,2000",
            "2016-3-28,004,http://flink.apache.org/,888",
            "2016-3-28,003,http://kafka.apache.org/,999",
            "2016-3-28,002,http://hive.apache.org/,77",
            "2016-3-28,004,http://parquet.apache.org/,66",
            "2016-3-28,003,http://spark.apache.org/,666"
        )
        //生成RDD分布式集合对象
        val userDataRDD = sc.parallelize(userData)
        //获取数据
        /**
         * 第四步：根据业务需要对数据进行预处理生成DataFrame，要想把RDD转换成DataFrame，需要先把RDD中的元素类型变成Row类型
         * 于此同时要提供DataFrame中的Columns的元数据信息描述
         */
        val userDataRDDRow = userDataRDD.map(row =>{val splited: Array[String]=row.split(",");
            Row(splited(0),splited(1).toInt,splited(2),splited(3).toInt)})
        //构造元数据
        val structTypes = StructType(Array(
        StructField("time",StringType,true),
        StructField("id",IntegerType,true),
        StructField("url",StringType,true),
        StructField("amount",IntegerType,true)
        ))

        //创建DataFrame
        val userDataDF = sqlContext.createDataFrame(userDataRDDRow,structTypes)
        /**
         * 第五步：使用Spark SQL提供的内置函数对DataFrame进行操作，特别注意：内置函数生成的Column对象且自定进行CG；
         */
        userDataDF.groupBy("time").agg('time,countDistinct('id))
            .map(row => Row(row(1),row(2)))
            .collect
            .foreach(println)

        //对销售进行统计
        userDataDF.groupBy("time").agg('time,sum('amount)).show()
//            .map(row => Row(row(1),row(2)))
//            .collect
//            .foreach(println)
    }
}
