/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.Schema;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/**
 * Display contents or checksums of files 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Display extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Cat.class, "-cat");
    factory.addClass(Text.class, "-text");
    factory.addClass(Checksum.class, "-checksum");
  }

  /**
   * Displays file content to stdout
   */
  public static class Cat extends Display {
    public static final String NAME = "cat";
    public static final String USAGE = "[-ignoreCrc] <src> ...";
    public static final String DESCRIPTION =
      "Fetch all files that match the file pattern <src> \n" +
      "and display their content on stdout.\n";

    private boolean verifyChecksum = true;

    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "ignoreCrc");
      cf.parse(args);
      verifyChecksum = !cf.getOpt("ignoreCrc");
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new PathIsDirectoryException(item.toString());
      }
      
      item.fs.setVerifyChecksum(verifyChecksum);
      printToStdout(getInputStream(item));
    }

    private void printToStdout(InputStream in) throws IOException {
      try {
        IOUtils.copyBytes(in, out, getConf(), false);
      } finally {
        in.close();
      }
    }

    protected InputStream getInputStream(PathData item) throws IOException {
      return item.fs.open(item.path);
    }
  }
  
  /**
   * Same behavior as "-cat", but handles zip and TextRecordInputStream
   * and Avro encodings. 
   */ 
  public static class Text extends Cat {
    public static final String NAME = "text";
    public static final String USAGE = Cat.USAGE;
    public static final String DESCRIPTION =
      "Takes a source file and outputs the file in text format.\n" +
      "The allowed formats are zip and TextRecordInputStream and Avro.";
    
    @Override
    protected InputStream getInputStream(PathData item) throws IOException {
      FSDataInputStream i = (FSDataInputStream)super.getInputStream(item);

      // Check type of stream first
      switch(i.readShort()) {
        case 0x1f8b: { // RFC 1952
          // Must be gzip
          i.seek(0);
          return new GZIPInputStream(i);
        }
        case 0x5345: { // 'S' 'E'
          // Might be a SequenceFile
          if (i.readByte() == 'Q') {
            i.close();
            return new TextRecordInputStream(item.stat);
          }
        }
        default: {
          // Check the type of compression instead, depending on Codec class's
          // own detection methods, based on the provided path.
          CompressionCodecFactory cf = new CompressionCodecFactory(getConf());
          CompressionCodec codec = cf.getCodec(item.path);
          if (codec != null) {
            i.seek(0);
            return codec.createInputStream(i);
          }
          break;
        }
        case 0x4f62: { // 'O' 'b'
          if (i.readByte() == 'j') {
            i.close();
            return new AvroFileInputStream(item.stat);
          }
          break;
        }
      }

      // File is non-compressed, or not a file container we know.
      i.seek(0);
      return i;
    }
  }
  
  public static class Checksum extends Display {
    public static final String NAME = "checksum";
    public static final String USAGE = "<src> ...";
    public static final String DESCRIPTION =
      "Dump checksum information for files that match the file\n" +
      "pattern <src> to stdout. Note that this requires a round-trip\n" +
      "to a datanode storing each block of the file, and thus is not\n" +
      "efficient to run on a large number of files. The checksum of a\n" +
      "file depends on its content, block size and the checksum\n" +
      "algorithm and parameters used for creating the file.";

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new PathIsDirectoryException(item.toString());
      }

      FileChecksum checksum = item.fs.getFileChecksum(item.path);
      if (checksum == null) {
        out.printf("%s\tNONE\t\n", item.toString());
      } else {
        String checksumString = StringUtils.byteToHexString(
            checksum.getBytes(), 0, checksum.getLength());
        out.printf("%s\t%s\t%s\n",
            item.toString(), checksum.getAlgorithmName(),
            checksumString);
      }
    }
  }

  protected class TextRecordInputStream extends InputStream {
    SequenceFile.Reader r;
    Writable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public TextRecordInputStream(FileStatus f) throws IOException {
      final Path fpath = f.getPath();
      final Configuration lconf = getConf();
      r = new SequenceFile.Reader(lconf, 
          SequenceFile.Reader.file(fpath));
      key = ReflectionUtils.newInstance(
          r.getKeyClass().asSubclass(Writable.class), lconf);
      val = ReflectionUtils.newInstance(
          r.getValueClass().asSubclass(Writable.class), lconf);
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    @Override
    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        byte[] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }

    @Override
    public void close() throws IOException {
      r.close();
      super.close();
    }
  }

  /**
   * This class transforms a binary Avro data file into an InputStream
   * with data that is in a human readable JSON format.
   */
  protected static class AvroFileInputStream extends InputStream {
    private int pos;
    private byte[] buffer;
    private ByteArrayOutputStream output;
    private FileReader fileReader;
    private DatumWriter<Object> writer;
    private JsonEncoder encoder;

    public AvroFileInputStream(FileStatus status) throws IOException {
      pos = 0;
      buffer = new byte[0];
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      fileReader =
        DataFileReader.openReader(new File(status.getPath().toUri()), reader);
      Schema schema = fileReader.getSchema();
      writer = new GenericDatumWriter<Object>(schema);
      output = new ByteArrayOutputStream();
      JsonGenerator generator =
        new JsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);
      MinimalPrettyPrinter prettyPrinter = new MinimalPrettyPrinter();
      prettyPrinter.setRootValueSeparator(System.getProperty("line.separator"));
      generator.setPrettyPrinter(prettyPrinter);
      encoder = EncoderFactory.get().jsonEncoder(schema, generator);
    }

    /**
     * Read a single byte from the stream.
     */
    @Override
    public int read() throws IOException {
      if (pos < buffer.length) {
        return buffer[pos++];
      }
      if (!fileReader.hasNext()) {
        return -1;
      }
      writer.write(fileReader.next(), encoder);
      encoder.flush();
      if (!fileReader.hasNext()) {
        // Write a new line after the last Avro record.
        output.write(System.getProperty("line.separator").getBytes());
        output.flush();
      }
      pos = 0;
      buffer = output.toByteArray();
      output.reset();
      return read();
    }

    /**
      * Close the stream.
      */
    @Override
    public void close() throws IOException {
      fileReader.close();
      output.close();
      super.close();
    }
  }
}
