package edu.nps.deep.be.avro;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by mike on 3/29/17.
 * run with spark-submit 
 */
abstract public class SparkReadAvroBase implements Serializable
{
  //static {
    //System.load(new java.io.File("/user/jmbailey/nativelibs/libbe_scan_jni.so.0.0.0").getAbsolutePath());
    //System.load("/user/jmbailey/nativelibs/libbe_scan_jni.so");
  //}

  /*
    This reads in the avro file (compressed) and presents it as uncompressed byte arrays, size = specified in Avro
     schema, or less if last record is short
  */
  abstract protected void init(String path);
  abstract protected BEAvroMetaData getMetadata();
  abstract protected Schema getSchema();
  abstract protected Function getMapReadFunction(final Accumulator<Double> byteCountAccumulator, final Broadcast<BEAvroMetaData> metaBroadcast);
  
  private final DecimalFormat longForm = new DecimalFormat("#,###");
  private final DecimalFormat sleepForm = new DecimalFormat("#.###");

  private final double SLEEPSECS = 3.d;
  private final double SIXTYFOURMEG = 64.d*1024.d*1024.d;
  private final double sleepFactor = (SLEEPSECS*1000.d)/SIXTYFOURMEG;   //64Mb gives 3000ms = 3 sec.

  private final String description = "Spark job to read in Avro file -- dummy processing simulation of processing loop.\n"+
                                     "Simulation sleep time = "+SLEEPSECS+" seconds per 64Mb of data";
  
  
  public SparkReadAvroBase(String path)
  {
    System.out.println(description);
    System.out.println(getClass().getSimpleName()+": input: "+path);

    SparkConf conf = new SparkConf().setAppName("SparkReadAvro");
    conf.setMaster("local[*]");
    JavaSparkContext cont = new JavaSparkContext(conf);

    init(path);
    
    // This is how we get this metadata out to every executor
    final Broadcast<BEAvroMetaData> metaBroadcast = cont.broadcast(getMetadata());

    // This is how we count the processed bytes
    // final LongAccumulator byteCountAccumulator = cont.sc().longAccumulator("bytecount");   this would be better, but it's newer spark
    final Accumulator<Double> byteCountAccumulator = cont.accumulator(0.0d);

    Schema schema = getSchema(); //from subclass
    Job job = getJob(schema);

    // This api doesn't seem to be terribly intuitive, to say the least.
    JavaPairRDD<AvroKey,NullWritable> pairRdd = cont.newAPIHadoopFile(
        path,
        AvroKeyInputFormat.class,
        AvroKey.class,
        NullWritable.class,
        job.getConfiguration());

    JavaRDD<AvroKey> keysRdd = pairRdd.keys();

    JavaRDD mappedRdd = keysRdd.map(getMapReadFunction(byteCountAccumulator, metaBroadcast));
    
    long start = System.currentTimeMillis();
    Object n = mappedRdd.count(); // this starts it off
    long end = System.currentTimeMillis();

    System.out.println("***************************************");
    System.out.println("Avro read complete: ");
    System.out.println(""+n+" records processed, "+longForm.format(byteCountAccumulator.value())+" decompressed bytes processed");
    System.out.println("processing time (h:m:s:ms) "+formatTime(end-start));
    System.out.println("***************************************");
  }

  private String formatTime(long msecs)
  {
    //return String.format("%1$tH:%1$tM:%1$tS", msecs);   
    // Compiler will take care of constant arithmetics
    if (24 * 60 * 60 * 1000 > msecs) {
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      return sdf.format(msecs);
    }
    else {
      SimpleDateFormat sdf = new SimpleDateFormat(":mm:ss.SSS");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

      // Keep long data type
      // Compiler will take care of constant arithmetics
      long hours = msecs / (60L * 60L * 1000L);
      return hours + sdf.format(msecs);
    }
  }
  
  protected void toBulkExtractor(byte[] data, long dsize, long sourceFileOffset, BEAvroMetaData metadata)
  {
    double sleepTimeMs = sleepFactor*dsize;
    StringBuilder sb = new StringBuilder();
    sb.append("toBulkExtractor(), ");
    sb.append(longForm.format(dsize));
    sb.append(" bytes at file offset ");
    sb.append(longForm.format(sourceFileOffset));
    sb.append(" sleep ");
    sb.append(sleepForm.format(sleepTimeMs/1000.d));
    sb.append(" seconds");
    System.out.println(sb.toString());

    try{Thread.sleep((long)sleepTimeMs);}catch(InterruptedException ex){}

  /*  BEScan scanner = new BEScan("email", data, dsize);

    Artifact artifact = scanner.next();  //looks like it doesn't have a good way to return null or not-found
    while(artifact.getArtifactClass() != null && artifact.getArtifactClass().length()>0) {
      System.out.println();
      System.out.println("artifact class: "+artifact.getArtifactClass());
      long bufferoffset = artifact.getBufferOffset();
      System.out.println("artifact bufferoffset: "+bufferoffset);
      System.out.println("artifact fileoffset: "+(bufferoffset+sourceFileOffset));
      System.out.println("artifact javaartifact: "+new String(artifact.javaArtifact()));
      //System.out.println("artifact javacontext: "+new String(artifact.javaContext()));

      artifact = scanner.next();
    }
    scanner.delete();
    */
  }

  private Job getJob(Schema avroSchema)
  {
    Job job;
    try {
      job = Job.getInstance();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    AvroJob.setInputKeySchema(job, avroSchema);
    return job;
  }
}

