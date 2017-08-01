package edu.nps.deep.be.avro;

public class AvroizeFile128M
{
  public static void main(String[] args)
  {

      String[] sa = App.getPaths(args, 2);
      AvroConfig128M cfg = new AvroConfig128M();
      try {
        cfg.init(sa[0], sa[1]);
        new AvroizeFile().avroIzeFile(cfg);
      } catch (Exception ex ) {
        System.out.println("Exception: "+ex.getClass().getSimpleName()+": "+ex.getLocalizedMessage());
      }
  }
}
