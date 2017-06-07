7 June 2017

This is a set of Java classes for reading and processing files in the Apache Avro format.  The final aim will be
to convert raw disk images into Avro format, then to write code which will run under Spark on an HDFS filesystem to
read disk images in the Avro format and process them through the be_scan implementation of bulk extractor routines.

In addition to standard Spark and Hadoop libraries, this code requires
	org.apache.avro:avro-mapred:1.8.1 and
	org.apache.avro.avro:1.8.1
libraries to deal with the avro format.  These libraries are documented here:
http://avro.apache.org/docs/1.8.1/

Avro is a compression scheme which makes use of a schema describing the data.  The schema is, itself, included in
an Avro file.  The Avro file then is a series of records, each of which is described by the schema.

The schema is compiled into Java classes which are used in the reading/writing routines.  There are 2 ways for this
conversion to happen.  The first is automatically, with a Maven plugin.  The second is manually, through the avro-tools-1.8.1.jar.  This process is described here:
http://avro.apache.org/docs/1.8.1/gettingstartedjava.html#Compiling+the+schema

Generated sources such as those created by compiling an Avro schema would not normally be included in a Git repo -- they are
generated during a build.  They are included here temporarily while an automatic build process is finalized.

This code was developed under the Intellij IDEA integrated development environment.  I intend to make it work through
a Gradle build file.

Mike Bailey
