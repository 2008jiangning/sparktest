package com.jn.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.param.JavaParams;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 16-6-14.
 */
public class SparkSqlWithJoin {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("SparkSqlWithJoin").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //json文件创建DataFrame
        DataFrame peoplesDF = sqlContext.read().json("/spark/sparkdata/peoples.json");
        //注册临时表
        peoplesDF.registerTempTable("peopleScores");

        //查询出成绩大于90的学生
        DataFrame execellectScoresDF = sqlContext.sql("select name , score from peopleScores where score > 90");

        List<String> execellentScoresList = execellectScoresDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception{
                return row.getAs("name");
            }
        }).collect();

//        动态组织Json
        List<String> peopleInformations = new ArrayList<String>();
        peopleInformations.add("{\"name\":\"Michael\",\"age\":20}");
        peopleInformations.add("{\"name\":\"Andy\",\"age\":17}");
        peopleInformations.add("{\"name\":\"Justin\",\"age\":19}");

//        通过内容来构建DataFrame
        JavaRDD<String> peopleInformationRDD = sc.parallelize(peopleInformations);
        DataFrame peopleInformattionDF = sqlContext.read().json(peopleInformationRDD);

//        注册临时表
        peopleInformattionDF.registerTempTable("peopleInformations");

        String sqlText = "select name, age from peopleInformations where name in(";
        for(int i=0;i<execellentScoresList.size();i++){
            sqlText += "'" + execellentScoresList.get(i) + "'";
            if(i<execellentScoresList.size() - 1){
                sqlText += ",";
            }
        }
        sqlText += ")";

//        创建DataFrame
        DataFrame execellentNameAgeDF = sqlContext.sql(sqlText);

        JavaPairRDD<String, Tuple2<Integer,Integer>> resultRDD = execellectScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>((String)row.getAs("name"),(int)row.getLong(1));
            }
        }).join(execellentNameAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
             @Override
             public Tuple2<String, Integer> call(Row row) throws Exception {
                 return new Tuple2<String, Integer>((String) row.getAs("name"), (int) row.getLong(1));
             }
         }

        ));

    JavaRDD<Row> resultRowRDD = resultRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
                                              @Override
                                              public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                                                  return RowFactory.create(tuple._1(), tuple._2()._2(), tuple._2()._1());
                                              }
                                          }
    );

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

//        构建StructType,用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame personDF = sqlContext.createDataFrame(resultRowRDD,structType);
        personDF.show();
        personDF.write().format("json").save("/spark/sparkdata/personResult");

    }
}
