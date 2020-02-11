package org.datadragon;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;

public class AggFlow {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        loadData(spark);

        spark.stop();
    }



    private static void loadData(SparkSession spark) throws AnalysisException {

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("MsIsdn",  DataTypes.StringType, true),
                DataTypes.createStructField("Imsi", DataTypes.StringType, true),
                DataTypes.createStructField("Apn", DataTypes.StringType, true),
                DataTypes.createStructField("Rnc", DataTypes.StringType, true),
                DataTypes.createStructField("SubMarket", DataTypes.StringType, true),
                DataTypes.createStructField("NodeB", DataTypes.StringType, true),
                DataTypes.createStructField("Eci", DataTypes.StringType, true),
                DataTypes.createStructField("BinStartTimeMicro", DataTypes.StringType, true),
                DataTypes.createStructField("EndTimeMicro", DataTypes.StringType, true),
                DataTypes.createStructField("WatchListType", DataTypes.StringType, true),
                DataTypes.createStructField("Domain", DataTypes.StringType, true),
                DataTypes.createStructField("CpPrefix", DataTypes.StringType, true),
                DataTypes.createStructField("DnsDomain", DataTypes.StringType, true),
                DataTypes.createStructField("Protocol", DataTypes.StringType, true),
                DataTypes.createStructField("SrcIp", DataTypes.StringType, true),
                DataTypes.createStructField("DstIp", DataTypes.StringType, true),
                DataTypes.createStructField("SrcPort", DataTypes.StringType, true),
                DataTypes.createStructField("DstPort", DataTypes.StringType, true),
                DataTypes.createStructField("Direction", DataTypes.StringType, true),
                DataTypes.createStructField("IBytes", DataTypes.LongType, true),
                DataTypes.createStructField("IPackets", DataTypes.LongType, true),
                DataTypes.createStructField("manufacturer", DataTypes.StringType, true),
                DataTypes.createStructField("model", DataTypes.StringType, true),
                DataTypes.createStructField("ActiveVector", DataTypes.StringType, true),
                DataTypes.createStructField("domain_number", DataTypes.StringType, true),
                DataTypes.createStructField("application_id", DataTypes.StringType, true)
        });
        Dataset<Row> df = spark.read().format("csv")
                .schema(schema)
                .option("header", true)
                .option("sep", "|")
                .option("inferSchema",true)
                .load("src/main/resources/TEST_BS_SFNSA_20200123.0000LT_1D.dat.gz");

        df = df.withColumn("BinStartTime", functions.substring(functions.col("BinStartTimeMicro").cast(DataTypes.LongType), 0, 10));
        df = df.withColumn("StartMinute", functions.date_trunc("minute", functions.from_unixtime(functions.col("BinStartTime"))));

        df.select("BinStartTime","StartMinute").show(3);
        Dataset<Row> df2=  df.groupBy(functions.col("Imsi"), functions.col("StartMinute")).count().sort("StartMinute");
        //df.createOrReplaceTempView("test");
        //Dataset<Row> df2 = spark.sql("select Imsi, StartMinute, count(Imsi) from test group by Imsi, StartMinute order by StartMinute" );
        df2.show(3);
        //df.withColumn("");
        //df.printSchema();
    }
}

