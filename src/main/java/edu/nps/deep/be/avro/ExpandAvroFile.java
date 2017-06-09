package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.schemas.DiskImageSplit;
import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static edu.nps.deep.be.avro.BEAvroConstants.DEFAULT_AVRO_FILE;
import static edu.nps.deep.be.avro.BEAvroConstants.DEFAULT_REWRITTEN_FILE;

public class ExpandAvroFile
{
  public static void main(String[] args)
  {
    String[] paths = App.getPaths(args,2);
    expandAvroFile(paths[0],paths[1]);
  }

  public static void expandAvroFile(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_AVRO_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_REWRITTEN_FILE;

    System.out.println("ExpandAvroFile: input: "+inputPath+" output: "+outputPath);
    int INP=0,OUTP=1;
    try {
      FilePack[] fileData = BeAvroUtils.getFileObjects(inputPath,outputPath);
      DatumReader<DiskImageSplit> diDatumReader = new SpecificDatumReader<>(DiskImageSplit.class);
      DataFileReader<DiskImageSplit> diFileReader = new DataFileReader<>(fileData[INP].inputFsInput,diDatumReader); //inf,diDatumReader);

      DiskImageSplit dis = null;
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      while(diFileReader.hasNext()) {
        dis = diFileReader.next(dis);
        long len = dis.getDatalength();
        byte[] ba = dis.getData().bytes();
        fileData[OUTP].outputStream.write(ba,0,(int)len); //fos.write(ba,0,(int)len);
        md5.update(ba,0,(int)len);
        System.out.print(".");
      }
      diFileReader.close();
      fileData[OUTP].outputStream.close();//fos.close();
      System.out.println();
      System.out.println("Output MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }
}
