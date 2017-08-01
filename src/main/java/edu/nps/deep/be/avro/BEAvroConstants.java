package edu.nps.deep.be.avro;

/**
 * Created by mike on 5/16/17.
 */
public class BEAvroConstants
{
  public static String BE_AVRO_VERSION = "1.0";

  public static String BE_AVRO_VERSION_META_KEY = "beAvroVersion";
  public static String SOURCE_FILE_LENGTH_META_KEY = "sourceFileLength";
  public static String SOURCE_FILE_PATH_META_KEY = "sourcePath";
  public static String SOURCE_FILE_MODTIME_META_KEY = "sourceFileLastModified";
  public static String AVRO_FILE_CREATION_DATE_META_KEY = "avroFileCreation";
  public static String AVRO_FILE_BUILDER_META_KEY = "avroBuiltBy";
  public static String AVRO_FILE_RECORD_BUFFER_SIZE_KEY = "avroRecordBufferSize";
  public static String AVRO_DATA_MD5 = "avroDataMd5";

  public static String DEFAULT_SAMPLE_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw";//"/Users/mike/Desktop/avrowork/firstimage.bin";
  public static String DEFAULT_AVRO_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw.avro"; //"/Users/mike/Desktop/avrowork/firstimage.avro";
  public static String DEFAULT_REWRITTEN_FILE = "/Users/mike/Desktop/encase.e01.work/e01.raw.avro.test"; //"/Users/mike/Desktop/avrowork/firstimage.new.bin";

  public static int SAMPLE_SPLIT_SIZE = 1024*1024*128; //test*128;  // 128Mb, must match schema size
  public static int SAMPLE_NUMBER_OF_SPLITS = 4;
  public static int SAMPLE_SIZE = SAMPLE_SPLIT_SIZE * SAMPLE_NUMBER_OF_SPLITS;
}
