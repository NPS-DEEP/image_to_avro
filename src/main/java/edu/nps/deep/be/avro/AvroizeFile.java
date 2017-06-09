package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.schemas.DiskImageSplit;
import edu.nps.deep.be.avro.schemas.SplitData;
import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
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

public class AvroizeFile
{
  public static void main(String[] args)
  {
    String[] sa = App.getPaths(args,2);
    new AvroizeFile().avroIzeFile(sa[0], sa[1]);
  }

  public void avroIzeFile(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_SAMPLE_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_AVRO_FILE;

    System.out.println("AvroizeFile: input: "+inputPath+" output: "+outputPath);
    int INP=0,OUTP=1;
    try {
      FilePack[] filedata = BeAvroUtils.getFileObjects(inputPath,outputPath);

      DatumWriter<DiskImageSplit> diDatumWriter = new SpecificDatumWriter<DiskImageSplit>(DiskImageSplit.class);
      DataFileWriter<DiskImageSplit> diFileWriter = new DataFileWriter<>(diDatumWriter);
      diFileWriter.setCodec(CodecFactory.snappyCodec());

      diFileWriter.create(new DiskImageSplit().getSchema(), filedata[OUTP].outputStream);
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      byte[] ba = new byte[SAMPLE_SPLIT_SIZE];
      long starttime = System.currentTimeMillis();
      long fileoffset = 0;
      int countRead = fillBuffer(fileoffset,ba,filedata[INP].inputStream);

      while(countRead != -1) {
        md5.update(ba, 0, countRead);
        if(countRead != SAMPLE_SPLIT_SIZE)
          for(int i=countRead;i<SAMPLE_SPLIT_SIZE;i++)
            ba[i] = 0; //zero out unread
        DiskImageSplit dis = new DiskImageSplit(fileoffset,(long)countRead,new SplitData(ba));
        diFileWriter.append(dis);
        System.out.print(".");
        fileoffset += countRead;

        countRead = fillBuffer(fileoffset,ba,filedata[INP].inputStream);
        System.out.println("."+" countRead: "+countRead);
      }
      String MD5String = new BigInteger(1,md5.digest()).toString(16).toUpperCase();
      addMeta(filedata[INP].inputFileStatus,diFileWriter,MD5String);

      filedata[INP].inputStream.close();
      diFileWriter.close();

      FileStatus outStatus = filedata[OUTP].fSys.getFileStatus(filedata[OUTP].p);

      long endtime = System.currentTimeMillis();
      long elapsed = endtime-starttime;
      long outputFilesize = outStatus.getLen();
      long inputFilesize = fileoffset;

      System.out.println();
      System.out.println("Input file: "+inputPath);
      System.out.println("Output file: "+outputPath);
      System.out.println("Split size: "+longForm.format(SAMPLE_SPLIT_SIZE));
      System.out.println("Time to compress "+longForm.format(inputFilesize)+" bytes = "+DurationFormatUtils.formatDuration(elapsed,"HH:mm:ss")+" (h:m:s)");
      System.out.println("Output size = "+longForm.format(outputFilesize));

      System.out.println("Compression ratio: "+ form.format((float)inputFilesize/outputFilesize)+":1");
      System.out.println("Input MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }

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
