package com.servebeer.djfil;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.functions;

public class SimpleApp {

  

  public static void main(String[] args) {
    //{"ClientAddr":"142.93.236.18:41694",
    // "ClientHost":"142.93.236.18",
    // "ClientPort":"41694",
    // "ClientUsername":"-",
    // "DownstreamContentSize":19,
    // "DownstreamStatus":404,
    // "Duration":176911,
    // "Overhead":176911,
    // "RequestAddr":"3.252.206.74:443",
    // "RequestContentSize":0,
    // "RequestCount":2193,
    // "RequestHost":"3.252.206.74",
    // "RequestMethod":"GET",
    // "RequestPath":"/",
    // "RequestPort":"443",
    // "RequestProtocol":"HTTP/1.1",
    // "RequestScheme":"http",
    // "RetryAttempts":0,
    // "StartLocal":"2023-07-18T08:03:58.009808429Z",
    // "StartUTC":"2023-07-18T08:03:58.009808429Z",
    // "level":"info",
    // "msg":"",
    // "time":"2023-07-18T08:03:58Z"}
    
    String logFile = "/tmp/data/traefik_access.log"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();
    Dataset<Row> logData = spark.read().json(logFile);

    logData.printSchema();
    //logData.show();

    RelationalGroupedDataset count_ips = logData.groupBy("ClientHost");
    Dataset<Row> ips = count_ips.count().sort(functions.desc("count"));
    ips.show();

    long uniques = logData.select("ClientHost").distinct().count();
    System.out.println("Unique IPs: " + uniques);

    spark.stop();
  }

}