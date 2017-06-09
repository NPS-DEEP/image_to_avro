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

  public static String[] getPaths(String[] args, int num)
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
      System.err.println("No console.");
      System.exit(1);
    }
    return console.readLine(prompt);
  }

}
