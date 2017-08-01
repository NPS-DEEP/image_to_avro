package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.BeAvroUtils.FilePack;
import edu.nps.deep.be.avro.schemas.DiskImageSplit63M;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;

import static edu.nps.deep.be.avro.BEAvroConstants.*;
import static edu.nps.deep.be.avro.BeAvroUtils.FilePack.*;

/**
 * Created by mike on 3/29/17. run with spark-submit
 */
public class SparkReadAvro63M extends SparkReadAvroBase
{
 public static void main(String[] args)
  {
    String[] sa = App.getPaths(args, 1);
    new SparkReadAvro63M(sa[0]);
  }

  private BEAvroMetaData metadata;

  public SparkReadAvro63M(String path)
  {
    super(path);
  }

  @Override
  protected void init(String path)
  {
    try {
      FilePack[] fileData = BeAvroUtils.getFileObjects(path);
      DatumReader<DiskImageSplit63M> diDatumReader = new SpecificDatumReader<>(DiskImageSplit63M.class);
      DataFileReader<DiskImageSplit63M> dfr = new DataFileReader<>(fileData[INP].inputFsInput, diDatumReader); //inf,diDatumReader);
      metadata = new BEAvroMetaData(); //default

      metadata = new BEAvroMetaData(
          dfr.getMetaString(SOURCE_FILE_PATH_META_KEY),
          dfr.getMetaString(SOURCE_FILE_LENGTH_META_KEY),
          dfr.getMetaString(SOURCE_FILE_MODTIME_META_KEY),
          dfr.getMetaString(AVRO_FILE_BUILDER_META_KEY),
          dfr.getMetaString(AVRO_FILE_CREATION_DATE_META_KEY),
          dfr.getMetaString(BE_AVRO_VERSION_META_KEY),
          dfr.getMetaString(AVRO_DATA_MD5));
    }
    catch (IOException ex) {
      System.err.println("Error retrieving meta data from " + path);
    }
  }

  @Override
  protected BEAvroMetaData getMetadata()
  {
    return metadata;
  }

  @Override
  protected Schema getSchema()
  {
    return AvroUtils.toSchema(DiskImageSplit63M.class.getName());
  }

  @Override
  protected Function getMapReadFunction(final Accumulator<Double> byteCountAccumulator, final Broadcast<BEAvroMetaData> metaBroadcast)
  {
    return new Function<AvroKey, Object>()
    {
      public Object call(AvroKey key)
      {
        DiskImageSplit63M dis = (DiskImageSplit63M) key.datum();
        toBulkExtractor(dis.getData().bytes(), dis.getDatalength(), dis.getFileoffset(), metaBroadcast.getValue());
        byteCountAccumulator.add((double) dis.getDatalength());
        return null;
      }
    };
  }
}
