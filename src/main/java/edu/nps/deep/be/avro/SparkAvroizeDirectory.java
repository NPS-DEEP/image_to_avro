package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.schemas.DiskImageSplit;
import edu.nps.deep.be.avro.schemas.SplitData;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static edu.nps.deep.be.avro.BEAvroConstants.*;

/**
 * Created by mike on 6/7/17.
 */
public class SparkAvroizeDirectory implements Serializable
{
  public static void main(String[] args)
  {
    System.out.println("Tom is wrong");
    String[] sa = App.getPaths(args,2);
    new SparkAvroizeDirectory().avroizeDirectory(sa[0], sa[1]);
  }

  public void avroizeDirectory(String inputPath, String outputPath)
  {
    System.out.println("SparkAvroizeDirectory: input: "+inputPath+" output: "+outputPath);
    int INP=0,OUTP=1;
    try {
      BeAvroUtils.FilePack[] filedata = BeAvroUtils.getFileObjectsNoStreams(inputPath, outputPath);
      if(!filedata[INP].fSys.isDirectory(filedata[INP].p)) {
        System.err.println("Input path is not a directory");
        System.exit(-1);
      }
      filedata[OUTP].fSys.mkdirs(filedata[OUTP].p); // ensure output dir exists

      if(!filedata[OUTP].fSys.isDirectory(filedata[OUTP].p)) {  // might be superfluous
        System.err.println("Outpath is not a directory");
        System.exit(-1);
      }

      FileSystem inpFs = filedata[INP].fSys;

      SparkConf conf = new SparkConf().setAppName("SparkAvroizeDirectory");
      //conf.setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      ArrayList<String> filePaths = new ArrayList<>();
      FileStatus[] fs = inpFs.listStatus(filedata[INP].p);
      if(fs == null || fs.length<=0) {
        System.err.println("No files found in "+inputPath);
        System.exit(-1);
      }
      int junk = 0;
      for(FileStatus fileS : fs) {
        if(!fileS.isFile())
          continue;
        filePaths.add(fileS.getPath().toString());
        junk++;
        if(junk>=2)
          break;
      }
      System.out.println(""+filePaths.size()+" files found in "+inputPath);

      // This is how we get the output path out to every executor
      final Broadcast<String> outputDirectoryPath = sc.broadcast(outputPath); // leave on the hdfs: filedata[OUTP].p.toString());

      JavaRDD<String> pathRDD = sc.parallelize(filePaths);
      JavaRDD<Object> mappedRDD = pathRDD.map( new Function<String,Object>()
      {
        public Object call(String pathString)
        {
          try {
            avroizeFile(pathString, outputDirectoryPath.getValue());
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
    System.out.println("Job done.");
  }

  private void avroizeFile(String p, String oPath) throws NoSuchAlgorithmException,IOException
  {
    FileSystem inpFS = BeAvroUtils.getFileSystem(p);
    Path inpP = new Path(p);
    FSDataInputStream inStream = inpFS.open(inpP);
    FileStatus fStatus = inpFS.getFileStatus(inpP);

    FileSystem outFS = BeAvroUtils.getFileSystem(oPath);
    Path outP = new Path(oPath, inpP.getName()+".avro");
    FSDataOutputStream outStream = outFS.create(outP,true);

    DatumWriter<DiskImageSplit> diDatumWriter = new SpecificDatumWriter<DiskImageSplit>(DiskImageSplit.class);
    DataFileWriter<DiskImageSplit> diFileWriter = new DataFileWriter<>(diDatumWriter);
    diFileWriter.setCodec(CodecFactory.snappyCodec());

    diFileWriter.create(new DiskImageSplit().getSchema(), outStream); //filedata[OUTP].outputStream);
    MessageDigest md5 = MessageDigest.getInstance("MD5");

    byte[] ba = new byte[SAMPLE_SPLIT_SIZE];
    long starttime = System.currentTimeMillis();
    long fileoffset = 0;
    int countRead = fillBuffer(fileoffset,ba,inStream); //filedata[INP].inputStream);

    while(countRead != -1) {
      md5.update(ba, 0, countRead);
      if(countRead != SAMPLE_SPLIT_SIZE)
        for(int i=countRead;i<SAMPLE_SPLIT_SIZE;i++)
          ba[i] = 0; //zero out unread
      DiskImageSplit dis = new DiskImageSplit(fileoffset,(long)countRead,new SplitData(ba));
      diFileWriter.append(dis);
      fileoffset += countRead;

      countRead = fillBuffer(fileoffset,ba,inStream); //filedata[INP].inputStream);
      //System.out.println("."+" countRead: "+countRead);
    }
    String MD5String = new BigInteger(1,md5.digest()).toString(16).toUpperCase();

    inStream.close(); //filedata[INP].inputStream.close();
    diFileWriter.close();
    addMeta(fStatus,diFileWriter,MD5String); //filedata[INP].inputFileStatus,diFileWriter,MD5String);

    FileStatus outStatus = outFS./*filedata[OUTP].fSys.*/getFileStatus(outP); //filedata[OUTP].p);

    long endtime = System.currentTimeMillis();
    long elapsed = endtime-starttime;
    long outputFilesize = outStatus.getLen();
    long inputFilesize = fileoffset;

    System.out.println("Input file: "+p); //inputPath);
    System.out.println("Output file: "+oPath); //outputPath);
    System.out.println("Split size: "+longForm.format(SAMPLE_SPLIT_SIZE));
    System.out.println("Time to compress "+longForm.format(inputFilesize)+" bytes = "+ DurationFormatUtils.formatDuration(elapsed,"HH:mm:ss")+" (h:m:s)");
    System.out.println("Output size = "+longForm.format(outputFilesize));

    System.out.println("Compression ratio: "+ form.format((float)inputFilesize/outputFilesize)+":1");
    System.out.println("Input MD5: "+MD5String);
  }

  // Code from AvroizeFile
  private int fillBuffer(long offset, byte[]ba,FSDataInputStream is)
  {
    int count = 0;
    while(count < ba.length) {
      int ret = -1;
      try {
        //System.out.println("Trying to read from "+(offset+count)+" to buffer offset "+count+" length "+(ba.length-count));
        ret = is.read(offset+count,ba,count,ba.length-count);
        //System.out.println("Read "+ret+" bytes");
      }
      catch(IOException ex) {
        System.err.println("IOException file read: "+ex.getLocalizedMessage());
        return -1;
      }
      if(ret == -1)
        return count>0?count:-1;
      count += ret;
    }
    return count;
  }

  private DecimalFormat form = new DecimalFormat("####.#");
  private DecimalFormat longForm = new DecimalFormat("#,###");

  private void addMeta(FileStatus inf, DataFileWriter<?> dfw, String md5)
  {
    DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
    String fLength = ""+inf.getLen();
    String modStamp = df.format(new Date(inf.getModificationTime()));
    String dateNow = df.format(new Date());
    String builtBy = AvroizeFile.class.getName();
    String path = inf.getPath().toString();

    dfw.setMeta(SOURCE_FILE_PATH_META_KEY,path);
    System.out.println("Avro meta data/path: "+path);
    dfw.setMeta(SOURCE_FILE_MODTIME_META_KEY,modStamp);
    System.out.println("Avro meta data/modtime: "+modStamp);
    dfw.setMeta(SOURCE_FILE_LENGTH_META_KEY,fLength);
    System.out.println("Avro meta data/length: "+fLength);
    dfw.setMeta(AVRO_FILE_CREATION_DATE_META_KEY,dateNow);
    System.out.println("Avro meta data/dateNow: "+dateNow);
    dfw.setMeta(AVRO_FILE_BUILDER_META_KEY,builtBy);
    System.out.println("Avro meta data/builtBy: "+builtBy);
    dfw.setMeta(BE_AVRO_VERSION_META_KEY,BE_AVRO_VERSION);
    System.out.println("Avro meta data/version: "+BE_AVRO_VERSION);
    dfw.setMeta(AVRO_DATA_MD5,md5);
    System.out.println("Avro original file MD5: "+md5);
  }

}


