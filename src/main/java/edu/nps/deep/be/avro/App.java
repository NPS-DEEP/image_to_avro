package edu.nps.deep.be.avro;

import java.io.Console;

/**
 * Hello world!
 */
public class App
{
  public static void main(String[] args)
  {
    System.out.println("Spark Avro Bulkextractor");
    System.out.println("Usage:");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar AvroizeFile");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar AvroizeFile inputpath outputpath");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar ExpandAvroFile");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar ExpandAvroFile inputpath outputpath");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar SparkReadAvro");
    System.out.println("java -jar SparkAvroBulkExtractorJDK1.7.jar SparkReadAvro inputpath");
  }

  static String[] getPaths(String[] args, int num)
  {
    if (args.length < num) {
      String inputpath = readFromConsole("Enter path to input file: ");
      String outputpath = null;
      if(num > 1)
        outputpath = readFromConsole("Entry path to output file: ");

      if(num == 1 && inputpath != null && inputpath.length()>0)
        return new String[]{inputpath};

      if (inputpath == null || inputpath.length() <= 0 || outputpath == null || outputpath.length() <= 0) {
        System.err.println("Error.  Must enter required file path(s)");
        System.exit(-1);
      }
      return new String[]{inputpath, outputpath};
    }
    else
      return args;
  }

  private static String readFromConsole(String prompt)
  {
    Console console = System.console();
    if (console == null) {
      System.err.println("No console, full paths must be specified");
      System.exit(1);
    }
    return console.readLine(prompt);
  }

  public static class AppArgs
  {
    public String inputpath;
    public String outputpath;
    public String thirdpath;
    public int max2Process = Integer.MAX_VALUE;
  }

  public static AppArgs get2PathsAndCount(String[] args)
  {
    String[] arr = getPaths(args, 2);
    AppArgs aa = new AppArgs();
    aa.inputpath = arr[0];
    aa.outputpath = arr[1];

    if(args.length>2) {
      try {
        aa.max2Process = Integer.parseInt(args[2]);
      }
      catch(Exception ex) {
        System.err.println("Error parsing integer (3rd argument)");
        System.exit(-1);
      }
    }
    return aa;
  }

  public static AppArgs get3PathsAndCount(String[] args)
  {
    String[] arr = getPaths(args, 3);
    AppArgs aa = new AppArgs();
    aa.inputpath = arr[0];
    aa.outputpath = arr[1];
    aa.thirdpath = arr[2];

    if(args.length>3) {
      try {
        aa.max2Process = Integer.parseInt(args[2]);
      }
      catch(Exception ex) {
        System.err.println("Error parsing integer (3rd argument)");
        System.exit(-1);
      }
    }
    return aa;

  }

}
