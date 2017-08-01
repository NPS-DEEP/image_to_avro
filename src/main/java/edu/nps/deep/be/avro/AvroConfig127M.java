package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import edu.nps.deep.be.avro.schemas.DiskImageSplit127M;
import edu.nps.deep.be.avro.schemas.SplitData127M;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;

import static edu.nps.deep.be.avro.BeAvroUtils.FilePack.*;

public class AvroConfig127M implements AvroApiConfig
{
  public static int PARTITIONSIZE = 1024*1024*127;
  private DataFileWriter<DiskImageSplit127M> diFileWriter;
  private FilePack[] filedata;

  @Override
  public void init(String inputPath, String outputPath) throws IOException
  {
    filedata = BeAvroUtils.getFileObjects(inputPath,outputPath);

    DatumWriter<DiskImageSplit127M> diDatumWriter = new SpecificDatumWriter<>(DiskImageSplit127M.class);

    diFileWriter = new DataFileWriter<>(diDatumWriter);
    diFileWriter.setCodec(CodecFactory.snappyCodec());
  }

  @Override
  public void prepare() throws IOException
  {
    diFileWriter.create(new DiskImageSplit127M().getSchema(), filedata[OUTP].outputStream);
  }

  @Override
  public FilePack[] getFileData()
  {
    return filedata;
  }

  @Override
  public void setMeta(String key, String value)
  {
    diFileWriter.setMeta(key,value);
  }

  @Override
  public void write(long fileoffset, long countRead, byte[] ba) throws IOException
  {
    DiskImageSplit127M dis = new DiskImageSplit127M(fileoffset,countRead,new SplitData127M(ba));    
    diFileWriter.append(dis);
  }

  @Override
  public int getPartitionSize()
  {
    return PARTITIONSIZE;
  }

  @Override
  public void close() throws IOException
  {
    filedata[INP].inputStream.close();
    System.out.println("AvroConfig127M.close(): closing diFileWriter");
    diFileWriter.close();

  }
}
