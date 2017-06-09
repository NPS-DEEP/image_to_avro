package edu.nps.deep.be.avro;

public class DiskImageMisc
{
/*  public static String DEFAULT_SAMPLE_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw";//"/Users/mike/Desktop/avrowork/firstimage.bin";
  public static String DEFAULT_AVRO_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw.avro"; //"/Users/mike/Desktop/avrowork/firstimage.avro";
  public static String DEFAULT_REWRITTEN_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw.avro.test"; //"/Users/mike/Desktop/avrowork/firstimage.new.bin";
  
  public static int SAMPLE_SPLIT_SIZE = 1024*1024; //test*128;  // 128Mb, must match schema size
  public static int SAMPLE_NUMBER_OF_SPLITS = 4;
  public static int SAMPLE_SIZE = SAMPLE_SPLIT_SIZE * SAMPLE_NUMBER_OF_SPLITS;

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
  
  public static void avroIzeFile_old(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_SAMPLE_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_AVRO_FILE;
    
    System.out.println("AvroIze: input: "+inputPath+" output: "+outputPath);
    
    try {
      File inf = new File(inputPath);
      FileInputStream fis = new FileInputStream(inf);
      
      DatumWriter<DiskImageSplitx> diDatumWriter = new SpecificDatumWriter<DiskImageSplitx>(DiskImageSplitx.class);
      DataFileWriter<DiskImageSplitx> diFileWriter = new DataFileWriter<>(diDatumWriter);
      diFileWriter.setCodec(CodecFactory.snappyCodec());
      
      File outf = new File(outputPath);
      diFileWriter.create(new DiskImageSplitx().getSchema(), outf);
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      byte[] ba = new byte[SAMPLE_SPLIT_SIZE];
      int countRead = fis.read(ba);
      while(countRead != -1) {
        md5.update(ba, 0, countRead);
        if(countRead != SAMPLE_SPLIT_SIZE)
          for(int i=countRead;i<SAMPLE_SPLIT_SIZE;i++)
            ba[i] = 0; //zero out unread
        DiskImageSplitx dis = new DiskImageSplitx(ba);
        diFileWriter.append(dis);
        System.out.println("avro written: "+ba.length+" bytes");

        countRead = fis.read(ba);
      } 
      fis.close();
      diFileWriter.close();
      System.out.println("Input MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }
  public static void avroIzeFile(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_SAMPLE_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_AVRO_FILE;
    
    System.out.println("AvroIze: input: "+inputPath+" output: "+outputPath);
    
    try {
      File inf = new File(inputPath);
      FileInputStream fis = new FileInputStream(inf);
      
      DatumWriter<DiskImageSplit2> diDatumWriter = new SpecificDatumWriter<DiskImageSplit2>(DiskImageSplit2.class);
      DataFileWriter<DiskImageSplit2> diFileWriter = new DataFileWriter<>(diDatumWriter);
      diFileWriter.setCodec(CodecFactory.snappyCodec());
      
      File outf = new File(outputPath);
      diFileWriter.create(new DiskImageSplit2().getSchema(), outf);
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      byte[] ba = new byte[SAMPLE_SPLIT_SIZE];
      int countRead = fis.read(ba);
      while(countRead != -1) {
        md5.update(ba, 0, countRead);
        if(countRead != SAMPLE_SPLIT_SIZE)
          for(int i=countRead;i<SAMPLE_SPLIT_SIZE;i++)
            ba[i] = 0; //zero out unread
        DiskImageSplitx dis = new DiskImageSplitx((long)countRead,new Splitdata(ba));
        diFileWriter.append(dis);
        System.out.println("avro written: "+ba.length+" bytes");

        countRead = fis.read(ba);
      } 
      fis.close();
      diFileWriter.close();
      System.out.println("Input MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }*/
  /*
  public static void uncompress_old(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_AVRO_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_REWRITTEN_FILE;
    
    System.out.println("UnAvroIze: input: "+inputPath+" output: "+outputPath);

    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      File outf = new File(outputPath);
      FileOutputStream fos = new FileOutputStream(outf);
      
      File inf = new File(inputPath);
      DatumReader<DiskImageSplitx> diDatumReader = new SpecificDatumReader<>(DiskImageSplitx.class);
      DataFileReader<DiskImageSplitx> diFileReader = new DataFileReader<>(inf,diDatumReader);
      
      DiskImageSplitx dis = null;
      while(diFileReader.hasNext()) {
        dis = diFileReader.next(dis);
        byte[] ba = dis.bytes();
        fos.write(ba);
        md5.update(ba);
      }
      diFileReader.close();
      fos.close();
      System.out.println("Output MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }
  public static void uncompress(String inputPath, String outputPath)
  {
    if(inputPath == null || inputPath.length()<=0)
      inputPath = DEFAULT_AVRO_FILE;
    if(outputPath == null || outputPath.length()<=0)
      outputPath = DEFAULT_REWRITTEN_FILE;
    
    System.out.println("UnAvroIze: input: "+inputPath+" output: "+outputPath);

    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      File outf = new File(outputPath);
      FileOutputStream fos = new FileOutputStream(outf);
      
      File inf = new File(inputPath);
      DatumReader<DiskImageSplitx> diDatumReader = new SpecificDatumReader<>(DiskImageSplitx.class);
      DataFileReader<DiskImageSplitx> diFileReader = new DataFileReader<>(inf,diDatumReader);
      
      DiskImageSplitx dis = null;
      while(diFileReader.hasNext()) {
        dis = diFileReader.next(dis);
        long len = dis.getDatalength();
        byte[] ba = dis.getData().bytes();
        fos.write(ba,0,(int)len);
        md5.update(ba,0,(int)len);
      }
      diFileReader.close();
      fos.close();
      System.out.println("Output MD5: "+new BigInteger(1,md5.digest()).toString(16).toUpperCase());
    }
    catch(IOException | NoSuchAlgorithmException ex) {
      System.out.println(ex);
    }
  }*/
}

