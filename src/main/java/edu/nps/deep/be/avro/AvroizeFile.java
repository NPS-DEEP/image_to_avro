package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static edu.nps.deep.be.avro.BEAvroConstants.*;
import static edu.nps.deep.be.avro.BeAvroUtils.FilePack.INP;
import static edu.nps.deep.be.avro.BeAvroUtils.FilePack.OUTP;

public class AvroizeFile
{
  public static void main(String[] args)
  {
    String[] sa = App.getPaths(args, 2);
    AvroConfig64M cfg = new AvroConfig64M();
    try {
      cfg.init(sa[0], sa[1]);
      new AvroizeFile().avroIzeFile(cfg);
    }
    catch (IOException | NoSuchAlgorithmException ex ) {

    }
  }
  
  private int sparkPartitionSize;
  public void avroIzeFile(AvroApiConfig cfg) throws IOException, NoSuchAlgorithmException
  {
    sparkPartitionSize = cfg.getPartitionSize();
    FilePack[] filedata = cfg.getFileData();
    System.out.println("AvroizeFile: input: "+filedata[INP].p+" output: "+filedata[OUTP].p+" partition size: "+sparkPartitionSize);
    
    addMeta(cfg); //must be done at the start
    cfg.prepare();
    
    MessageDigest md5 = MessageDigest.getInstance("MD5");

    byte[] ba = new byte[sparkPartitionSize];
    long starttime = System.currentTimeMillis();
    long fileoffset = 0;
    int countRead = fillBuffer(fileoffset,ba,filedata[INP].inputStream);

    while(countRead != -1) {
      md5.update(ba, 0, countRead);
      if(countRead != sparkPartitionSize)
        for(int i=countRead;i<sparkPartitionSize;i++)
          ba[i] = 0; //zero out unread

      cfg.write(fileoffset,(long)countRead,ba);

      fileoffset += countRead;
      countRead = fillBuffer(fileoffset,ba,filedata[INP].inputStream);
      System.out.println("countRead: "+countRead);
    }
    cfg.close();

    String MD5String = new BigInteger(1,md5.digest()).toString(16).toUpperCase();
    //addMeta(cfg,MD5String); must be done at the start

    FileStatus outStatus = filedata[OUTP].fSys.getFileStatus(filedata[OUTP].p);

    long endtime = System.currentTimeMillis();
    long elapsed = endtime-starttime;
    long outputFilesize = outStatus.getLen();
    long inputFilesize = fileoffset;

    System.out.println();
    System.out.println("Input file: "+filedata[INP].p);
    System.out.println("Output file: "+filedata[OUTP].p);
    System.out.println("Split size: "+longForm.format(sparkPartitionSize));
    System.out.println("Time to compress "+longForm.format(inputFilesize)+" bytes = "+DurationFormatUtils.formatDuration(elapsed,"HH:mm:ss")+" (h:m:s)");
    System.out.println("Output size = "+longForm.format(outputFilesize));

    System.out.println("Compression ratio: "+ form.format((float)inputFilesize/outputFilesize)+":1");
    System.out.println("Input MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
  }

  private int fillBuffer(long offset, byte[]ba,FSDataInputStream is)
  {
     int count = 0;
     while(count < ba.length) {
       int ret;
       try {
         ret = is.read(offset+count,ba,count,ba.length-count);
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

  private final DecimalFormat form = new DecimalFormat("####.#");
  private final DecimalFormat longForm = new DecimalFormat("#,###");

  private void addMeta(AvroApiConfig cfg)//, String md5)
  {
    FileStatus inf = cfg.getFileData()[INP].inputFileStatus;
    DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
    String fLength = ""+inf.getLen();
    String modStamp = df.format(new Date(inf.getModificationTime()));
    String dateNow = df.format(new Date());
    String builtBy = AvroizeFile.class.getName();
    String path = inf.getPath().toString();
    String buffSize = ""+cfg.getPartitionSize();
    
    cfg.setMeta(SOURCE_FILE_PATH_META_KEY,path);
      System.out.println("Avro meta data/path: "+path);
    cfg.setMeta(SOURCE_FILE_MODTIME_META_KEY,modStamp);
      System.out.println("Avro meta data/modtime: "+modStamp);
    cfg.setMeta(SOURCE_FILE_LENGTH_META_KEY,fLength);
      System.out.println("Avro meta data/length: "+fLength);
    cfg.setMeta(AVRO_FILE_RECORD_BUFFER_SIZE_KEY,buffSize);
      System.out.println("Avro meta data/record buffer size: "+buffSize);
    cfg.setMeta(AVRO_FILE_CREATION_DATE_META_KEY,dateNow);
      System.out.println("Avro meta data/dateNow: "+dateNow);
    cfg.setMeta(AVRO_FILE_BUILDER_META_KEY,builtBy);
      System.out.println("Avro meta data/builtBy: "+builtBy);
    cfg.setMeta(BE_AVRO_VERSION_META_KEY,BE_AVRO_VERSION);
      System.out.println("Avro meta data/version: "+BE_AVRO_VERSION);
    //cfg.setMeta(AVRO_DATA_MD5,md5);
    //  System.out.println("Avro original file MD5: "+md5);
  }
}
