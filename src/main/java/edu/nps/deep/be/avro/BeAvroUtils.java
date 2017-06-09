package edu.nps.deep.be.avro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.avro.mapred.FsInput;
import java.io.IOException;
import java.net.URI;

/**
 * Created by mike on 5/30/17.
 */
public class BeAvroUtils
{
  public static class FilePack
  {
    URI uri;
    FileSystem fSys;
    Path p;
    FSDataInputStream inputStream;
    FileStatus inputFileStatus;
    FsInput inputFsInput;
    FSDataOutputStream outputStream;
  }

  /* Even numbers get inputStream, odds outputStream */
  public static FilePack[] getFileObjects(String ... paths) throws IOException
  {
    FilePack[] arr = new FilePack[paths.length];
    for(int i=0;i<paths.length;i++) {
      FilePack pack;
      if(paths[i] == null) {
        pack = null;
      }
      else {
        pack = new FilePack();
        pack.uri = URI.create(paths[i]);
        pack.fSys = getFS(pack.uri);
        pack.p = new Path(pack.uri.getPath());

        if(i%2 == 0) {
          pack.inputStream = pack.fSys.open(pack.p);
          pack.inputFileStatus = pack.fSys.getFileStatus(pack.p);
          pack.inputFsInput = new FsInput(pack.p,pack.fSys);
        }
        else
          pack.outputStream = pack.fSys.create(pack.p,true);
      }
      arr[i] = pack;
    }
    return arr;
  }

  private static FileSystem getFS(URI uri) throws IOException
  {
    if(uri.getScheme() == null)
      return FileSystem.getLocal(new Configuration());

    return FileSystem.get(uri,new Configuration());
  }
}
