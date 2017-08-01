package edu.nps.deep.be.avro;

import java.io.IOException;

public interface AvroApiConfig
{
  public void init(String inputpath, String outputpath) throws IOException;
  public void prepare() throws IOException;
  public void write(long fileoffset, long countread, byte[]ba) throws IOException;
  public int getPartitionSize();
  public BeAvroUtils.FilePack[] getFileData();
  public void setMeta(String key, String value);
  public void close() throws IOException;
}
