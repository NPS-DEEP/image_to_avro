package edu.nps.deep.be.avro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by mike on 3/29/17.
 * run with spark-submit --class edu.nps.jmbailey.spark.SparkBenchMark GraceJavaAppsJDK1.7.jar
 */
public class SparkBenchMark
{
  public final static String INPUTFILE = "hdfs:///user/jmbailey/GigFile.txt";
  public final static    int MIN_PARTITIONS = 4;

  public static void main(String[] args)
  {
    System.out.println("SparkBenchmark");
    SparkConf conf = new SparkConf().setAppName("SparkBenchMark");
    JavaSparkContext cont = new JavaSparkContext(conf);

    // start loop
    JavaRDD<String> fileRDD = cont.textFile(INPUTFILE); //,MIN_PARTITIONS);

    JavaRDD<String> filteredRDD = fileRDD.filter(new Function<String,Boolean>()
    {
      public Boolean call(String s) { return s.length()>0;}
    });

    System.out.println("Start count action");
    float startNano = System.nanoTime();
    long count = filteredRDD.count();
    float endNano = System.nanoTime();
    // end loop

    cont.stop();

    System.out.println("Filter test: count: "+count+", time: "+((endNano-startNano)/1000000000.)+" sec");
  }
}
