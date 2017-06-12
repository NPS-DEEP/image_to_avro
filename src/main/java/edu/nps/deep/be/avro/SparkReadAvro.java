package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import edu.nps.deep.be.avro.schemas.DiskImageSplit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
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

import static edu.nps.deep.be.avro.BEAvroConstants.*;

/**
 * Created by mike on 3/29/17.
 * run with spark-submit --class edu.nps.jmbailey.spark.SparkBenchMark GraceJavaAppsJDK1.7.jar
 */
public class SparkReadAvro implements Serializable
{
  static {
    //System.load(new java.io.File("/user/jmbailey/nativelibs/libbe_scan_jni.so.0.0.0").getAbsolutePath());
    System.load("/user/jmbailey/nativelibs/libbe_scan_jni.so");
  }

  public static void main(String[] args)
  {
    new SparkReadAvro().sparkReadAvro(App.getPaths(args,1)[0]);
  }

  /*
    This reads in the avro file (compressed) and presents it as uncompressed byte arrays, size = specified in Avro
     schema, or less if last record is short
  */

  public void sparkReadAvro(String path)
  {
    System.out.println("SparkReadAvro: input: "+path);

    SparkConf conf = new SparkConf().setAppName("SparkReadAvro");
    conf.setMaster("local[*]");
    JavaSparkContext cont = new JavaSparkContext(conf);
    int INP = 0;
    BEAvroMetaData metadata = new BEAvroMetaData(); //default
    try {
      FilePack[] fileData = BeAvroUtils.getFileObjects(path);
      DatumReader<DiskImageSplit> diDatumReader = new SpecificDatumReader<>(DiskImageSplit.class);
      DataFileReader<DiskImageSplit> dfr = new DataFileReader<>(fileData[INP].inputFsInput,diDatumReader); //inf,diDatumReader);

      metadata = new BEAvroMetaData(
          dfr.getMetaString(SOURCE_FILE_PATH_META_KEY),
          dfr.getMetaString(SOURCE_FILE_LENGTH_META_KEY),
          dfr.getMetaString(SOURCE_FILE_MODTIME_META_KEY),
          dfr.getMetaString(AVRO_FILE_BUILDER_META_KEY),
          dfr.getMetaString(AVRO_FILE_CREATION_DATE_META_KEY),
          dfr.getMetaString(BE_AVRO_VERSION_META_KEY),
          dfr.getMetaString(AVRO_DATA_MD5));
    }
    catch(IOException ex) {
      System.err.println("Error retrieving meta data from "+path);
    }
    System.out.println(metadata);

    // This is how we get this metadata out to every executor
    final Broadcast<BEAvroMetaData> metaBroadcast = cont.broadcast(metadata);

    // This is how we count the processed bytes
    // final LongAccumulator byteCountAccumulator = cont.sc().longAccumulator("bytecount");   this would be better, but it's newer spark
    final Accumulator<Double> byteCountAccumulator = cont.accumulator(0.0d);

    Schema schema = AvroUtils.toSchema(DiskImageSplit.class.getName());
    Job job = getJob(schema);

    // This api doesn't seem to be terribly intuitive, to say the least.
    JavaPairRDD<AvroKey,NullWritable> pairRdd = cont.newAPIHadoopFile(
        path,
        AvroKeyInputFormat.class,
        AvroKey.class,
        NullWritable.class,
        job.getConfiguration());

    JavaRDD<AvroKey> keysRdd = pairRdd.keys();

    JavaRDD mappedRdd = keysRdd.map(
      new Function<AvroKey,Object>()
      {
        public Object call(AvroKey key)
        {
          DiskImageSplit dis = (DiskImageSplit)key.datum();
          toBulkExtractor(dis.getData().bytes(),dis.getDatalength(),dis.getFileoffset(),metaBroadcast.getValue());
          byteCountAccumulator.add((double)dis.getDatalength());
          return null;
        }
      }
    );
    Object n = mappedRdd.count(); // this starts it off

    System.out.println("sparkReadAvro() complete: "+n+" avro records processed");
    System.out.println(longForm.format(byteCountAccumulator.value())+" bytes processed");
  }

  private DecimalFormat longForm = new DecimalFormat("#,###");

  private void toBulkExtractor(byte[] data, long dsize, long sourceFileOffset, BEAvroMetaData metadata)
  {
    System.out.println("toBulkExtractor(): data, "+longForm.format(dsize)+" at file offset "+longForm.format(sourceFileOffset));
    System.out.println("meta:"+metadata);
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

