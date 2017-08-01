package edu.nps.deep.be.avro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by mike on 6/7/17.
 */
public class SparkAvroizeE01Directory implements Serializable
{
  public static void main(String[] args)
  {
    App.AppArgs appArgs = App.get3PathsAndCount(args);
    new SparkAvroizeE01Directory().avroizeE01Directory(appArgs.inputpath,appArgs.outputpath,appArgs.thirdpath,appArgs.max2Process);
  }
  private int INP=0,OUTP=1;
  public void avroizeE01Directory(String inputPath, String outputPath, String ewfexporter, int maxFiles)
  {
    System.out.println("SparkAvroizeE01Directory: input: "+inputPath+" output: "+outputPath);
    try {
      BeAvroUtils.FilePack[] filedata = BeAvroUtils.getFileObjectsNoStreams(inputPath,outputPath);
      ArrayList<String> filePaths = BeAvroUtils.getFileList(filedata,maxFiles);

      filedata[OUTP].fSys.mkdirs(filedata[OUTP].p); // ensure output dir exists

      if(!filedata[OUTP].fSys.isDirectory(filedata[OUTP].p)) {  // might be superfluous
        System.err.println("Outpath is not a directory");
        System.exit(-1);
      }

      SparkConf conf = new SparkConf().setAppName("SparkAvroizeE01Directory");
      //!!!!!!!!comment for grace:
      conf.setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      // This is how we get the output path out to every executor
      final Broadcast<String> outputDirectoryPath = sc.broadcast(outputPath); // leave on the "hdfs:"
      final Broadcast<String> ewfexportPath = sc.broadcast(ewfexporter);

      JavaRDD<String> pathRDD = sc.parallelize(filePaths);
      JavaRDD<Object> mappedRDD = pathRDD.map( new Function<String,Object>()
      {
        public Object call(String pathString)
        {
          try {
            E01ToAvro.E01ToAvro(pathString,outputDirectoryPath.getValue(),ewfexportPath.getValue());
          }
          catch(Exception ex) {
            System.err.println("Exception ex: "+ex.getLocalizedMessage());
            ex.printStackTrace();
          }
          return null;
        }
      });
      mappedRDD.count(); //do it
    }

    catch(Exception ex) {
      System.err.println("Exception: "+ex.getClass().getSimpleName());
    }
    System.out.println("SparkAvroizeE01Directory job done.");
  }

}


