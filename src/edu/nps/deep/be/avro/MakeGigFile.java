package edu.nps.deep.be.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class MakeGigFile
{
  public static String subdirectory = "work/";
  public static String filename = "GigFile";
  public static String filetype = ".txt";

  private int gigs = 2;

  public static void main(String[] args)
  {
    new MakeGigFile(args);
  }
  public MakeGigFile(String[] args)
  {
    System.out.println("MakeGigFile");
    if(!parseArgs(args)) {
      System.out.println("Usage: 'java -jar blah.jar targetdir n', where n = size in gigabytes");
      System.exit(0);
    }

    makeFile();
  }

  private boolean parseArgs(String[] args)
  {
    if(args.length != 2)
      return false;
    subdirectory = args[0];
    try {
      gigs = Integer.parseInt(args[1]);
      return gigs>0;
    }
    catch(Throwable t) {
      return false;
    }
  }
  private static byte lf = (byte) System.getProperty("line.separator").charAt(0);
  private byte[] bytes = new byte[64*8*128]; // 64 k
  private void makeFile()
  {
     buildBuffer();
     FileOutputStream fos=null;
     try {
       long sz = gigs * 1073741824l;
       int loops = (int) (sz / bytes.length);
       File f = nameFile();
       new File(subdirectory).mkdirs();
       f.createNewFile();
       long totalWritten = 0;
       fos = new FileOutputStream(f);
       for (int i = 0; i < loops; i++) {
         fos.write(bytes);
         totalWritten += bytes.length;
         if (totalWritten % (50 * 0x1000000) == 0)
           System.out.println("" + totalWritten + " written");
       }
       fos.close();
       System.out.println("File "+f.getAbsolutePath()+" written, size = "+totalWritten);

     }
     catch(IOException ex) {
       if(fos != null)
         try {fos.close();}catch(IOException ioex){}
        System.out.println("Error: "+ex.getLocalizedMessage());
     }
  }

  private void buildBuffer()
  {
    int i = 0;
    int loops = bytes.length / 64;
    for (int x = 0; x < loops; x++) {
      for (int j = 65; j <= 90; j++, i++)   //A-Z
        bytes[i] = (byte) j;
      bytes[i++] = 32; //space
      for (int j = 97; j <= 122; j++, i++)  //a-z
        bytes[i] = (byte) j;
      // thats 26+1+26 = 53;
      for (int j = 48; j <= 57; j++, i++)   //0-9
        bytes[i] = (byte) j;
      // 63
      bytes[i++] = lf;
      // 64
    }

  }

  private File nameFile()
  {
     String nm = filename;
     int count = 1;
     File f = null;
     do {
       f = new File(subdirectory+nm+filetype);
       nm = filename+"_"+count++;
     }
     while(f.exists());
     return f;
  }
}
