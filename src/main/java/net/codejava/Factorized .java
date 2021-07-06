/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.codejava;

import java.util.List;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 *
 * @author ahmed eltabakh
 */
public class Factorized {
    
    public static void main(String[] args) {
        
        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Wuzzuf Jobs Project").master ("local[6]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        Dataset<Row> wuzzufDF = dataFrameReader.csv ("F:\\ITI\\Java SE\\archive\\Wuzzuf_Jobs.csv");
         
        wuzzufDF= wuzzufDF.select ("Title","Company","Location","Type","Level","YearsExp","Country","Skills");

        //Factorization
        
        StringIndexerModel labelIndexer = new StringIndexer()
               .setInputCol("YearsExp")
               .setOutputCol("factorizedYearsExp")
               .fit(wuzzufDF);
        
        Dataset<Row> factorizedWuzzufDF = labelIndexer.transform(wuzzufDF);
        
        factorizedWuzzufDF.show();
    
}
}