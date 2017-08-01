package edu.nps.deep.be.avro;

import edu.nps.deep.be.avro.schemas.DiskImageSplit;
import edu.nps.deep.be.avro.schemas.SplitData;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

import static edu.nps.deep.be.avro.BEAvroConstants.SAMPLE_SPLIT_SIZE;

/**
 * Created by mike on 6/13/17.
 */
public class E01ToAvro
{
  public static void main(String[] args)
  {
    System.out.println("E01ToAvro: "+args[0] + args[1] + args[2]);
    try {
      E01ToAvro(args[0],args[1],args[2]);
    }
    catch(IOException ex) {
      System.err.println("IOException: "+ex.getLocalizedMessage());
    }
  }

  public static void E01ToAvro(String inputFilePath, String outputDirectoryPath, String ewfexportPath) throws IOException
  {
    Path inpP = new Path(inputFilePath);
//this works, but how to pass a hdfs path to ewfexport?
    ProcessBuilder pb = new ProcessBuilder(ewfexportPath, "-u","-t", "-", inpP.toUri().getPath());  //means send to stdout, only the path, not scheme

    FileSystem outFS = BeAvroUtils.getFileSystem(outputDirectoryPath);
    Path outP = new Path(outputDirectoryPath, inpP.getName()+".avro");
    FSDataOutputStream outStream = outFS.create(outP,true);

    DatumWriter<DiskImageSplit> diDatumWriter = new SpecificDatumWriter<>(DiskImageSplit.class);
    DataFileWriter<DiskImageSplit> diFileWriter = new DataFileWriter<>(diDatumWriter);
    diFileWriter.setCodec(CodecFactory.snappyCodec());

    diFileWriter.create(new DiskImageSplit().getSchema(), outStream);

    // Start ewfexport
    Process p = pb.start();
    E01ToAvro(p.getInputStream(),diFileWriter); // this is the output of the subprocess
    System.out.println("Job done.");
  }

  public static void E01ToAvro(InputStream instream, DataFileWriter<DiskImageSplit> dwriter) throws IOException
  {
    byte[] ba = new byte[SAMPLE_SPLIT_SIZE];

    long fileoffset = 0;
    int countRead = fillBuffer(ba,instream);

    while(countRead != -1) {
      if(countRead != SAMPLE_SPLIT_SIZE)
        for(int i=countRead;i<SAMPLE_SPLIT_SIZE;i++)
          ba[i] = 0; //zero out unread
      DiskImageSplit dis = new DiskImageSplit(fileoffset,(long)countRead,new SplitData(ba));
      dwriter.append(dis);
      fileoffset += countRead;

      countRead = fillBuffer(ba,instream);;
    }

    instream.close();
    dwriter.close();
  }

  // Code from AvroizeFile
  private static int fillBuffer(byte[]ba, InputStream is)
  {
    int totalRead = 0;
    while(totalRead < ba.length) {
      int numRead;
      try {
        numRead = is.read(ba,totalRead,ba.length-totalRead);
      }
      catch(IOException ex) {
        System.err.println("IOException file read: "+ex.getLocalizedMessage());
        return -1;
      }
      if(numRead == -1)
        return totalRead>0 ? totalRead : -1;
      totalRead += numRead;
    }
    return totalRead;
  }
}
