package edu.nps.deep.be.avro;

import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

/**
 * Created by mike on 5/30/17.
 */
public class BeAvroUtils
{
  public static class FilePack
  {
    public static int INP = 0;
    public static int OUTP = 1;

    URI uri;
    FileSystem fSys;
    Path p;
    FSDataInputStream inputStream;
    FileStatus inputFileStatus;
    FsInput inputFsInput;
    FSDataOutputStream outputStream;
  }

  /* Even numbers get inputStream, odds outputStream */
  public static FilePack[] getFileObjectsNoStreams(String... paths) throws IOException
  {
    return _getFileObjects(false,paths);
  }

  public static FilePack[] getFileObjects(String ... paths) throws IOException
  {
    return _getFileObjects(true,paths);
  }

  private static FilePack[] _getFileObjects(boolean makeStreams, String ... paths) throws IOException
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
          pack.inputFileStatus = pack.fSys.getFileStatus(pack.p);
          if (makeStreams) {
            pack.inputStream = pack.fSys.open(pack.p);
            pack.inputFsInput = new FsInput(pack.p, new Configuration()); //pack.fSys.getConf());
          }
        }
        else
          if(makeStreams) pack.outputStream = pack.fSys.create(pack.p,true);
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

  public static FileSystem getFileSystem(String pathString) throws IOException
  {
    URI uri = URI.create(pathString);
    return getFS(uri);
  }

  public static Object getFileList(BeAvroUtils.FilePack[] filedata) throws IOException
  {
    return getFileList(filedata,Integer.MAX_VALUE);
  }

  public static ArrayList<String> getFileList(BeAvroUtils.FilePack[] filedata, int maxFiles) throws IOException
  {
    int INP = 0, OUTP = 1;

    if (!filedata[INP].fSys.isDirectory(filedata[INP].p)) {
      System.err.println("Input path is not a directory");
      System.exit(-1);
    }

    FileSystem inpFs = filedata[INP].fSys;

    ArrayList<String> filePaths = new ArrayList<>();
    FileStatus[] fs = inpFs.listStatus(filedata[INP].p);
    if (fs == null || fs.length <= 0) {
      System.err.println("No files found in " + filedata[INP].uri);
      System.exit(-1);
    }
    int fcount = 0;
    for (FileStatus fileS : fs) {
      if (!fileS.isFile())
        continue;
      if (fileExists(fileS, filedata[OUTP]))
        continue;
      filePaths.add(fileS.getPath().toString());
      fcount++;
      if (fcount >= maxFiles)
        break;
    }
    System.out.println("" + filePaths.size() + " unconverted files found in " + filedata[INP].uri);
    return filePaths;
  }

  private static boolean fileExists(FileStatus inFs, BeAvroUtils.FilePack outPack)
  {
    String inputName = inFs.getPath().getName();
    Path outpath = new Path(outPack.p,inputName+".avro");
    try {
      return outPack.fSys.exists(outpath);
    }
    catch(IOException ex) {
      System.err.println("Error when checking if output file exists -- "+outpath.toString()+" : "+ex.getLocalizedMessage());
      System.exit(-1);
      return true; // not used but avoids ide error
    }
  }
}
