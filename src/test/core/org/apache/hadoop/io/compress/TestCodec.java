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
package org.apache.hadoop.io.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestCodec {

  private static final Log LOG= 
    LogFactory.getLog(TestCodec.class);

  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();

  @Test
  public void testDefaultCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DefaultCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DefaultCodec");
  }

  @Test
  public void testGzipCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  public void testBZip2Codec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
  }

  @Test
  public void testGzipCodecWithParam() throws IOException {
    Configuration conf = new Configuration(this.conf);
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.BEST_COMPRESSION);
    ZlibFactory.setCompressionStrategy(conf, CompressionStrategy.HUFFMAN_ONLY);
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
  }

  private static void codecTest(Configuration conf, int seed, int count, 
                                String codecClass) 
    throws IOException {
    
    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
        ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    LOG.info("Created a Codec object of type: " + codecClass);

    // Generate data
    DataOutputBuffer data = new DataOutputBuffer();
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for(int i=0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      
      key.write(data);
      value.write(data);
    }
    DataInputBuffer originalData = new DataInputBuffer();
    DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData));
    originalData.reset(data.getData(), 0, data.getLength());
    
    LOG.info("Generated " + count + " records");
    
    // Compress data
    DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    CompressionOutputStream deflateFilter = 
      codec.createOutputStream(compressedDataBuffer);
    DataOutputStream deflateOut = 
      new DataOutputStream(new BufferedOutputStream(deflateFilter));
    deflateOut.write(data.getData(), 0, data.getLength());
    deflateOut.flush();
    deflateFilter.finish();
    LOG.info("Finished compressing data");
    
    // De-compress data
    DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, 
                                 compressedDataBuffer.getLength());
    CompressionInputStream inflateFilter = 
      codec.createInputStream(deCompressedDataBuffer);
    DataInputStream inflateIn = 
      new DataInputStream(new BufferedInputStream(inflateFilter));

    // Check
    for(int i=0; i < count; ++i) {
      RandomDatum k1 = new RandomDatum();
      RandomDatum v1 = new RandomDatum();
      k1.readFields(originalIn);
      v1.readFields(originalIn);
      
      RandomDatum k2 = new RandomDatum();
      RandomDatum v2 = new RandomDatum();
      k2.readFields(inflateIn);
      v2.readFields(inflateIn);
    }
    LOG.info("SUCCESS! Completed checking " + count + " records");
  }

  @Test
  public void testSplitableCodecs() throws Exception {
    testSplitableCodec(BZip2Codec.class);
  }

  private void testSplitableCodec(
      Class<? extends SplittableCompressionCodec> codecClass)
      throws IOException {
    final long DEFLBYTES = 2 * 1024 * 1024;
    final Configuration conf = new Configuration();
    final Random rand = new Random();
    final long seed = rand.nextLong();
    LOG.info("seed: " + seed);
    rand.setSeed(seed);
    SplittableCompressionCodec codec =
      ReflectionUtils.newInstance(codecClass, conf);
    final FileSystem fs = FileSystem.getLocal(conf);
    final FileStatus infile =
      fs.getFileStatus(writeSplitTestFile(fs, rand, codec, DEFLBYTES));
    if (infile.getLen() > Integer.MAX_VALUE) {
      fail("Unexpected compression: " + DEFLBYTES + " -> " + infile.getLen());
    }
    final int flen = (int) infile.getLen();
    final Text line = new Text();
    final Decompressor dcmp = CodecPool.getDecompressor(codec);
    try {
      for (int pos = 0; pos < infile.getLen(); pos += rand.nextInt(flen / 8)) {
        // read from random positions, verifying that there exist two sequential
        // lines as written in writeSplitTestFile
        final SplitCompressionInputStream in =
          codec.createInputStream(fs.open(infile.getPath()), dcmp,
              pos, flen, SplittableCompressionCodec.READ_MODE.BYBLOCK);
        if (in.getAdjustedStart() >= flen) {
          break;
        }
        LOG.info("SAMPLE " + in.getAdjustedStart() + "," + in.getAdjustedEnd());
        final LineReader lreader = new LineReader(in);
        lreader.readLine(line); // ignore; likely partial
        if (in.getPos() >= flen) {
          break;
        }
        lreader.readLine(line);
        final int seq1 = readLeadingInt(line);
        lreader.readLine(line);
        if (in.getPos() >= flen) {
          break;
        }
        final int seq2 = readLeadingInt(line);
        assertEquals("Mismatched lines", seq1 + 1, seq2);
      }
    } finally {
      CodecPool.returnDecompressor(dcmp);
    }
    // remove on success
    fs.delete(infile.getPath().getParent(), true);
  }

  private static int readLeadingInt(Text txt) throws IOException {
    DataInputStream in =
      new DataInputStream(new ByteArrayInputStream(txt.getBytes()));
    return in.readInt();
  }

  /** Write infLen bytes (deflated) to file in test dir using codec.
   * Records are of the form
   * &lt;i&gt;&lt;b64 rand&gt;&lt;i+i&gt;&lt;b64 rand&gt;
   */
  private static Path writeSplitTestFile(FileSystem fs, Random rand,
      CompressionCodec codec, long infLen) throws IOException {
    final int REC_SIZE = 1024;
    final Path wd = new Path(new Path(
          System.getProperty("test.build.data", "/tmp")).makeQualified(fs),
        codec.getClass().getSimpleName());
    final Path file = new Path(wd, "test" + codec.getDefaultExtension());
    final byte[] b = new byte[REC_SIZE];
    final Base64 b64 = new Base64();
    DataOutputStream fout = null;
    Compressor cmp = CodecPool.getCompressor(codec);
    try {
      fout = new DataOutputStream(codec.createOutputStream(
            fs.create(file, true), cmp));
      final DataOutputBuffer dob = new DataOutputBuffer(REC_SIZE * 4 / 3 + 4);
      int seq = 0;
      while (infLen > 0) {
        rand.nextBytes(b);
        final byte[] b64enc = b64.encode(b); // ensures rand printable, no LF
        dob.reset();
        dob.writeInt(seq);
        System.arraycopy(dob.getData(), 0, b64enc, 0, dob.getLength());
        fout.write(b64enc);
        fout.write('\n');
        ++seq;
        infLen -= b64enc.length;
      }
      LOG.info("Wrote " + seq + " records to " + file);
    } finally {
      IOUtils.cleanup(LOG, fout);
      CodecPool.returnCompressor(cmp);
    }
    return file;
  }

  @Test
  public void testCodecPoolGzipReuse() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", true);
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      LOG.warn("testCodecPoolGzipReuse skipped: native libs not loaded");
      return;
    }
    GzipCodec gzc = ReflectionUtils.newInstance(GzipCodec.class, conf);
    DefaultCodec dfc = ReflectionUtils.newInstance(DefaultCodec.class, conf);
    Compressor c1 = CodecPool.getCompressor(gzc);
    Compressor c2 = CodecPool.getCompressor(dfc);
    CodecPool.returnCompressor(c1);
    CodecPool.returnCompressor(c2);
    assertTrue("Got mismatched ZlibCompressor", c2 != CodecPool.getCompressor(gzc));
  }

  private static void gzipReinitTest(Configuration conf, CompressionCodec codec)
      throws IOException {
    // Add codec to cache
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.BEST_COMPRESSION);
    ZlibFactory.setCompressionStrategy(conf,
        CompressionStrategy.DEFAULT_STRATEGY);
    Compressor c1 = CodecPool.getCompressor(codec);
    CodecPool.returnCompressor(c1);
    // reset compressor's compression level to perform no compression
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.NO_COMPRESSION);
    Compressor c2 = CodecPool.getCompressor(codec, conf);
    // ensure same compressor placed earlier
    assertTrue("Got mismatched ZlibCompressor", c1 == c2);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    CompressionOutputStream cos = null;
    // write trivially compressable data
    byte[] b = new byte[1 << 15];
    Arrays.fill(b, (byte) 43);
    try {
      cos = codec.createOutputStream(bos, c2);
      cos.write(b);
    } finally {
      if (cos != null) {
        cos.close();
      }
      CodecPool.returnCompressor(c2);
    }
    byte[] outbytes = bos.toByteArray();
    // verify data were not compressed
    assertTrue("Compressed bytes contrary to configuration",
               outbytes.length >= b.length);
  }

  @Test
  public void testCodecPoolCompressorReinit() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", true);
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      GzipCodec gzc = ReflectionUtils.newInstance(GzipCodec.class, conf);
      gzipReinitTest(conf, gzc);
    } else {
      LOG.warn("testCodecPoolCompressorReinit skipped: native libs not loaded");
    }
    conf.setBoolean("hadoop.native.lib", false);
    DefaultCodec dfc = ReflectionUtils.newInstance(DefaultCodec.class, conf);
    gzipReinitTest(conf, dfc);
  }

  @Test
  public void testSequenceFileDefaultCodec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 1000000);
  }

  @Test
  public void testSequenceFileBZip2Codec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000);
  }

  private static void sequenceFileCodecTest(Configuration conf, int lines, 
                                String codecClass, int blockSize) 
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

    Path filePath = new Path("SequenceFileCodecTest." + codecClass);
    // Configuration
    conf.setInt("io.seqfile.compress.blocksize", blockSize);
    
    // Create the SequenceFile
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Creating SequenceFile with codec \"" + codecClass + "\"");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath, 
        Text.class, Text.class, CompressionType.BLOCK, 
        (CompressionCodec)Class.forName(codecClass).newInstance());
    
    // Write some data
    LOG.info("Writing to SequenceFile...");
    for (int i=0; i<lines; i++) {
      Text key = new Text("key" + i);
      Text value = new Text("value" + i);
      writer.append(key, value);
    }
    writer.close();
    
    // Read the data back and check
    LOG.info("Reading from the SequenceFile...");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
    
    Writable key = (Writable)reader.getKeyClass().newInstance();
    Writable value = (Writable)reader.getValueClass().newInstance();
    
    int lc = 0;
    try {
      while (reader.next(key, value)) {
        assertEquals("key" + lc, key.toString());
        assertEquals("value" + lc, value.toString());
        lc ++;
      }
    } finally {
      reader.close();
    }
    assertEquals(lines, lc);

    // Delete temporary files
    fs.delete(filePath, false);

    LOG.info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + "\"");
  }
  
  public static void main(String[] args) {
    int count = 10000;
    String codecClass = "org.apache.hadoop.io.compress.DefaultCodec";

    String usage = "TestCodec [-count N] [-codec <codec class>]";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    try {
      for (int i=0; i < args.length; ++i) {       // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-codec")) {
          codecClass = args[++i];
        }
      }

      Configuration conf = new Configuration();
      int seed = 0;
      codecTest(conf, seed, count, codecClass);
    } catch (Exception e) {
      System.err.println("Caught: " + e);
      e.printStackTrace();
    }
    
  }

}
