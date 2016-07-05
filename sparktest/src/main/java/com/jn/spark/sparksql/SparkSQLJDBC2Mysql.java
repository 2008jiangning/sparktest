package com.jn.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by root on 16-6-14.
 */
public class SparkSQLJDBC2Mysql {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("SparkSQLJDBC2Mysql").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://hadoop1:3306/spark");//数据库URL
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password", "123456");
        reader.option("dbtable","dtspark");//table name

        //创建DF这个时候并不真正执行，lazy级别，基于dtspark表创建DataFrame
        DataFrame dtSparkDataSourceDFFromMysql = reader.load();
        /**
         * 在实际的企业级开发环境中，如果数据库中数据规模特别大，例如10亿条数据，此时采用传统的DB去处理的话
         * 一般需要对10亿条数据分成很多批次处理，例如分成100批（受限于单台Server的处理能力），且实际的处理过程
         * 可能会非常复杂，通过传统的Java EE等技术可能很难或者不方便实现处理算法，此时采用Spark SQL获得数据库
         * 中的数据并进行分布式处理就可以非常好的解决该问题，但是由于Spark SQL加载DB中的数据需要时间，所以一般
         * 会在Spark SQL和具体要操作的DB之间加上一个缓冲层次，例如中间使用Redis，可以把Spark 处理速度提高到
         * 甚至45倍；		 *
         */
        reader.option("dbtable","dthadoop");//数据库表名
//        基于dthadoop表创建DataFrame
        DataFrame dthaddoopDataSourceDFFromMysql = reader.load();

        JavaPairRDD<String, Tuple2<Integer,Integer>> resultRDD = dtSparkDataSourceDFFromMysql.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>((String)row.getAs("name"),(int)row.getInt(1));
            }
        }).join(dthaddoopDataSourceDFFromMysql.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>((String) row.getAs("name"), (int) row.getInt(1));
            }
        }));

        JavaRDD<Row> resultRowRDD = resultRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1(), tuple._2()._2(), tuple._2()._1());
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType,true));

        //构建元数据信息
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame personDF = sqlContext.createDataFrame(resultRowRDD,structType);

        personDF.show();
        /**
          * 1,当DataFrame要把通过Spark SQL、Core、ML等复杂操作后的数据写入数据库的时候首先是权限的问题，必须
          * 		确保数据库授权了当前操作Spark SQL的用户；
          * 2，DataFrame要写数据到DB的时候一般都不可以直接写进去，而是要转成RDD，通过RDD写数据到DB中
          *
          */
        personDF.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                Connection connection = null;
                Statement statement = null;
                try{
                    connection = DriverManager.getConnection("jdbc:mysql://hadoop1:3306/spark","root","123456");
                    statement = connection.createStatement();
                    while (rowIterator.hasNext()){
                        String sql = "insert into nameAgeScore(name,age,score) values (";
                        Row row = rowIterator.next();
                        String name = row.getAs("name");
                        int age = row.getInt(1);
                        int score = row.getInt(2);
                        sql += "'"+name + "'," +"'"+ age + "'," + "'" + score + "')";
                        statement.execute(sql);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    if(statement!= null){
                        statement.close();
                    }
                    if(connection != null){
                        connection.close();
                    }
                }
            }
        });
    }
}