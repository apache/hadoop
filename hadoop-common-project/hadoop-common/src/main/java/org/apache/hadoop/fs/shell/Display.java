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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

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
      "Fetch all files that match the file pattern <src> " +
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
      // Always do sequential reads;
      return item.openForSequentialIO();
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

      // Handle 0 and 1-byte files
      short leadBytes;
      try {
        leadBytes = i.readShort();
      } catch (EOFException e) {
        i.seek(0);
        return i;
      }

      // Check type of stream first
      switch(leadBytes) {
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
    public static final String USAGE = "[-v] <src> ...";
    public static final String DESCRIPTION =
      "Dump checksum information for files that match the file " +
      "pattern <src> to stdout. Note that this requires a round-trip " +
      "to a datanode storing each block of the file, and thus is not " +
      "efficient to run on a large number of files. The checksum of a " +
      "file depends on its content, block size and the checksum " +
      "algorithm and parameters used for creating the file.";

    private boolean displayBlockSize;

    @Override
    protected void processOptions(LinkedList<String> args)
        throws IOException {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "v");
      cf.parse(args);
      displayBlockSize = cf.getOpt("v");
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new PathIsDirectoryException(item.toString());
      }

      FileChecksum checksum = item.fs.getFileChecksum(item.path);
      String outputChecksum = checksum == null ? "NONE" :
          String.format("%s\t%s", checksum.getAlgorithmName(), StringUtils
              .byteToHexString(checksum.getBytes(), 0, checksum.getLength()));
      if (displayBlockSize) {
        FileStatus fileStatus = item.fs.getFileStatus(item.path);
        out.printf("%s\t%s\tBlockSize=%s%n", item.toString(), outputChecksum,
            fileStatus != null ? fileStatus.getBlockSize() : "NONE");
      } else {
        out.printf("%s\t%s%n", item.toString(), outputChecksum);
      }
    }
  }

  protected class TextRecordInputStream extends InputStream {
    private final SequenceFile.Reader r;
    private Object key;
    private Object val;

    private final DataInputBuffer inbuf;
    private final DataOutputBuffer outbuf;

    public TextRecordInputStream(FileStatus f) throws IOException {
      final Path fpath = f.getPath();
      final Configuration lconf = getConf();
      r = new SequenceFile.Reader(lconf, 
          SequenceFile.Reader.file(fpath));
      key = ReflectionUtils.newInstance(r.getKeyClass(), lconf);
      val = ReflectionUtils.newInstance(r.getValueClass(), lconf);
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    @Override
    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!readNextFromSequenceFile()) {
          ret = -1;
        } else {
          ret = inbuf.read();
        }
      }
      return ret;
    }

    @Override
    public int read(byte[] dest, int destPos, int destLen) throws IOException {
      validateInputStreamReadArguments(dest, destPos, destLen);

      if (destLen == 0) {
        return 0;
      }

      int bytesRead = 0;
      while (destLen > 0) {
        // Attempt to copy buffered data.
        int copyLen = inbuf.read(dest, destPos, destLen);
        if (-1 == copyLen) {
          // There was no buffered data.
          if (!readNextFromSequenceFile()) {
            // There is also no data remaining in the file.
            break;
          }
          // Reattempt copy now that we have buffered data.
          copyLen = inbuf.read(dest, destPos, destLen);
        }
        bytesRead += copyLen;
        destPos += copyLen;
        destLen -= copyLen;
      }

      return bytesRead > 0 ? bytesRead : -1;
    }

    @Override
    public void close() throws IOException {
      r.close();
      super.close();
    }

    private boolean readNextFromSequenceFile() throws IOException {
      key = r.next(key);
      if (key == null) {
        return false;
      } else {
        val = r.getCurrentValue(val);
      }
      byte[] tmp = key.toString().getBytes(StandardCharsets.UTF_8);
      outbuf.write(tmp, 0, tmp.length);
      outbuf.write('\t');
      tmp = val.toString().getBytes(StandardCharsets.UTF_8);
      outbuf.write(tmp, 0, tmp.length);
      outbuf.write('\n');
      inbuf.reset(outbuf.getData(), outbuf.getLength());
      outbuf.reset();
      return true;
    }
  }

  /**
   * This class transforms a binary Avro data file into an InputStream
   * with data that is in a human readable JSON format.
   */
  protected static class AvroFileInputStream extends InputStream {
    private int pos;
    private byte[] buffer;
    private final ByteArrayOutputStream output;
    private final FileReader<?> fileReader;
    private final DatumWriter<Object> writer;
    private final JsonEncoder encoder;
    private final byte[] finalSeparator;

    public AvroFileInputStream(FileStatus status) throws IOException {
      pos = 0;
      buffer = new byte[0];
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      FileContext fc = FileContext.getFileContext(new Configuration());
      fileReader =
        DataFileReader.openReader(new AvroFSInput(fc, status.getPath()),reader);
      Schema schema = fileReader.getSchema();
      writer = new GenericDatumWriter<Object>(schema);
      output = new ByteArrayOutputStream();
      encoder = EncoderFactory.get().jsonEncoder(schema, output);
      finalSeparator = System.getProperty("line.separator").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Read a single byte from the stream.
     */
    @Override
    public int read() throws IOException {
      if (buffer == null) {
        return -1;
      }

      if (pos < buffer.length) {
        return buffer[pos++];
      }

      if (!fileReader.hasNext()) {
        // Unset buffer to signal EOF on future calls.
        buffer = null;
        return -1;
      }

      writer.write(fileReader.next(), encoder);
      encoder.flush();

      if (!fileReader.hasNext()) {
        if (buffer.length > 0) {
          // Write a new line after the last Avro record.
          output.write(finalSeparator);
          output.flush();
        }
      }

      swapBuffer();
      return read();
    }

    @Override
    public int read(byte[] dest, int destPos, int destLen) throws IOException {
      validateInputStreamReadArguments(dest, destPos, destLen);

      if (destLen == 0) {
        return 0;
      }

      if (buffer == null) {
        return -1;
      }

      int bytesRead = 0;
      while (destLen > 0 && buffer != null) {
        if (pos < buffer.length) {
          // We have buffered data available, either from the Avro file or the final separator.
          int copyLen = Math.min(buffer.length - pos, destLen);
          System.arraycopy(buffer, pos, dest, destPos, copyLen);
          pos += copyLen;
          bytesRead += copyLen;
          destPos += copyLen;
          destLen -= copyLen;
        } else if (buffer == finalSeparator) {
          // There is no buffered data, and the last buffer processed was the final separator.
          // Unset buffer to signal EOF on future calls.
          buffer = null;
        } else if (!fileReader.hasNext()) {
          if (buffer.length > 0) {
            // There is no data remaining in the file. Get ready to write the final separator on
            // the next iteration.
            buffer = finalSeparator;
            pos = 0;
          } else {
            // We never read data into the buffer. This must be an empty file.
            // Immediate EOF, no separator needed.
            buffer = null;
            return -1;
          }
        } else {
          // Read the next data from the file into the buffer.
          writer.write(fileReader.next(), encoder);
          encoder.flush();
          swapBuffer();
        }
      }

      return bytesRead;
    }

    private void swapBuffer() {
      pos = 0;
      buffer = output.toByteArray();
      output.reset();
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

  private static void validateInputStreamReadArguments(byte[] dest, int destPos, int destLen)
      throws IOException {
    if (dest == null) {
      throw new NullPointerException("null destination buffer");
    } else if (destPos < 0 || destLen < 0 || destLen > dest.length - destPos) {
      throw new IndexOutOfBoundsException(String.format(
          "invalid destination buffer range: destPos = %d, destLen = %d", destPos, destLen));
    }
  }
}
