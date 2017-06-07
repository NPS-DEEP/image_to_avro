package edu.nps.deep.be.avro;

import java.io.Serializable;

/**
 * Created by mike on 5/18/17.
 */
public class BEAvroMetaData implements Serializable
{
  public String sourcePath="";
  public String sourceLength="";
  public String sourceModificationDate="";
  public String avroBuilder="";
  public String avroFileCreationDate="";
  public String beAvroVersion="";
  public String dataMD5;

  public BEAvroMetaData(){}

  public BEAvroMetaData(
      String sourcePath,
      String sourceLength,
      String sourceModificationDate,
      String avroBuilder,
      String avroFileCreationDate,
      String beAvroVersion,
      String dataMD5)
  {
    this.sourcePath=sourcePath;
    this.sourceLength=sourceLength;
    this.sourceModificationDate=sourceModificationDate;
    this.avroBuilder=avroBuilder;
    this.avroFileCreationDate=avroFileCreationDate;
    this.beAvroVersion=beAvroVersion;
    this.dataMD5=dataMD5;
  }

  @Override
  public String toString()
  {
    return String.format("%s %s %s %s %s %s %s",sourcePath,sourceLength,sourceModificationDate,avroBuilder,avroFileCreationDate,beAvroVersion,dataMD5);
  }
}
