package edu.nps.deep.be.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static edu.nps.deep.be.avro.BEAvroConstants.*;

// Not run on hdfs
public class MakeSampleDiskImage
{
  public static void main(String[] args)
  {
    makeSampleDiskImage(null);
  }

  public static void makeSampleDiskImage(String path)
  {
    if (path == null || path.length() <= 0)
      path = DEFAULT_SAMPLE_FILE;

    byte[] ba = new byte[SAMPLE_SPLIT_SIZE];
    byte b = 0;
    for (int i = 0; i < SAMPLE_SPLIT_SIZE; i++){
      ba[i] = b++;
      ba[i] = saltIt(ba[i]);
    }

    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      File f = new File(path);
      if (!f.exists())
        f.createNewFile();
      FileOutputStream fos = new FileOutputStream(f);
      for (int i = 0; i < SAMPLE_NUMBER_OF_SPLITS; i++) {
        fos.write(ba);
        md5.update(ba);
      }
      fos.close();
      System.out.println("MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch (IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }

  private static Random ran = new Random();
  private static byte saltIt(byte b)
  {
    int i = ran.nextInt(8);
    if(b%(i+1) == 0)
      return (byte)(b & (1<<i));
    return b;
  }
}
