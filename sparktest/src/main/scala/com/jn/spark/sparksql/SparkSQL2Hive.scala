package com.jn.spark.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 16-6-15.
 */
object SparkSQL2Hive {
    def main(args: Array[String]) {

        val conf = new SparkConf()
        conf.setAppName("SparkSQL2Hive").setMaster("local")
        val sc = new SparkContext(conf)
        try{
            val hiveContext = new HiveContext(sc)
            hiveContext.sql("use hive")//已经创建数据库hive
            //如果表存在删除
            hiveContext.sql("DROP TABLE IF EXISTS people")
            //创建内部表,姓名年龄
            hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
            //加载本地数据到数据仓库中
            hiveContext.sql("LOAD DATA LOCAL INPATH '/spark/sparkdata/people.txt' INTO TABLE people")

            //创建学生成绩表
            hiveContext.sql("DROP TABLE IF EXISTS peoplescores")
            hiveContext.sql("CREATE TABLE IF NOT EXISTS peoplescores(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
            hiveContext.sql("LOAD DATA LOCAL INPATH '/spark/sparkdata/peoplesscores.txt' INTO TABLE peoplescores ")

            val resultDF = hiveContext.sql("SELECT p.name,p.age,ps.score FROM people p JOIN peoplescores ps ON p.name = ps.name where ps.score > 90")

            //删除相同的表
            hiveContext.sql("DROP TABLE IF EXISTS peopleinfomationresult")
            resultDF.write.saveAsTable("peopleinfomationresult")
            val dataFrameHive = hiveContext.table("peopleinfomationresult")
            dataFrameHive.show()
        }catch{
            case e :Exception => e.printStackTrace()
        }
    }
}
