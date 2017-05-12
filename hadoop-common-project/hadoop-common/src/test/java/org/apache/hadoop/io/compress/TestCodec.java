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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestCodec {

  private static final Log LOG= LogFactory.getLog(TestCodec.class);

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

  @Test(timeout=20000)
  public void testBZip2Codec() throws IOException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "java-builtin");
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
  }
  
  @Test(timeout=20000)
  public void testBZip2NativeCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "system-native");
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
        codecTest(conf, seed, count, 
                  "org.apache.hadoop.io.compress.BZip2Codec");
        conf.set("io.compression.codec.bzip2.library", "java-builtin");
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
        codecTest(conf, seed, count, 
                  "org.apache.hadoop.io.compress.BZip2Codec");
      } else {
        LOG.warn("Native hadoop library available but native bzip2 is not");
      }
    }
  }
  
  @Test
  public void testSnappyCodec() throws IOException {
    if (SnappyCodec.isNativeCodeLoaded()) {
      codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.SnappyCodec");
      codecTest(conf, seed, count, "org.apache.hadoop.io.compress.SnappyCodec");
    }
  }
  
  @Test
  public void testLz4Codec() throws IOException {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      if (Lz4Codec.isNativeCodeLoaded()) {
        conf.setBoolean(
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
            false);
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
        codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
        conf.setBoolean(
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
            true);
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
        codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
      } else {
        Assert.fail("Native hadoop library available but lz4 not");
      }
    }
  }

  @Test
  public void testDeflateCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DeflateCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DeflateCodec");
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
    LOG.info("Generated " + count + " records");
    
    // Compress data
    DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    int leasedCompressorsBefore = codec.getCompressorType() == null ? -1
        : CodecPool.getLeasedCompressorsCount(codec);
    try (CompressionOutputStream deflateFilter =
      codec.createOutputStream(compressedDataBuffer);
      DataOutputStream deflateOut =
        new DataOutputStream(new BufferedOutputStream(deflateFilter))) {
      deflateOut.write(data.getData(), 0, data.getLength());
      deflateOut.flush();
      deflateFilter.finish();
    }
    if (leasedCompressorsBefore > -1) {
      assertEquals("leased compressor not returned to the codec pool",
          leasedCompressorsBefore, CodecPool.getLeasedCompressorsCount(codec));
    }
    LOG.info("Finished compressing data");
    
    // De-compress data
    DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, 
                                 compressedDataBuffer.getLength());
    DataInputBuffer originalData = new DataInputBuffer();
    int leasedDecompressorsBefore =
        CodecPool.getLeasedDecompressorsCount(codec);
    try (CompressionInputStream inflateFilter =
      codec.createInputStream(deCompressedDataBuffer);
      DataInputStream inflateIn =
        new DataInputStream(new BufferedInputStream(inflateFilter))) {

      // Check
      originalData.reset(data.getData(), 0, data.getLength());
      DataInputStream originalIn =
          new DataInputStream(new BufferedInputStream(originalData));
      for(int i=0; i < count; ++i) {
        RandomDatum k1 = new RandomDatum();
        RandomDatum v1 = new RandomDatum();
        k1.readFields(originalIn);
        v1.readFields(originalIn);
      
        RandomDatum k2 = new RandomDatum();
        RandomDatum v2 = new RandomDatum();
        k2.readFields(inflateIn);
        v2.readFields(inflateIn);
        assertTrue("original and compressed-then-decompressed-output not equal",
                   k1.equals(k2) && v1.equals(v2));
      
        // original and compressed-then-decompressed-output have the same
        // hashCode
        Map<RandomDatum, String> m = new HashMap<RandomDatum, String>();
        m.put(k1, k1.toString());
        m.put(v1, v1.toString());
        String result = m.get(k2);
        assertEquals("k1 and k2 hashcode not equal", result, k1.toString());
        result = m.get(v2);
        assertEquals("v1 and v2 hashcode not equal", result, v1.toString());
      }
    }
    assertEquals("leased decompressor not returned to the codec pool",
        leasedDecompressorsBefore,
        CodecPool.getLeasedDecompressorsCount(codec));

    // De-compress data byte-at-a-time
    originalData.reset(data.getData(), 0, data.getLength());
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, 
                                 compressedDataBuffer.getLength());
    try (CompressionInputStream inflateFilter =
      codec.createInputStream(deCompressedDataBuffer);
      DataInputStream originalIn =
        new DataInputStream(new BufferedInputStream(originalData))) {

      // Check
      int expected;
      do {
        expected = originalIn.read();
        assertEquals("Inflated stream read by byte does not match",
            expected, inflateFilter.read());
      } while (expected != -1);
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
    final Base64 b64 = new Base64(0, null);
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
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, true);
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

  private static void codecTestWithNOCompression (Configuration conf,
                      String codecClass) throws IOException {
    // Create a compressor with NO_COMPRESSION and make sure that
    // output is not compressed by comparing the size with the
    // original input

    CompressionCodec codec = null;
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.NO_COMPRESSION);
    try {
      codec = (CompressionCodec)
        ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    Compressor c = codec.createCompressor();
    // ensure same compressor placed earlier
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    CompressionOutputStream cos = null;
    // write trivially compressable data
    byte[] b = new byte[1 << 15];
    Arrays.fill(b, (byte) 43);
    try {
      cos = codec.createOutputStream(bos, c);
      cos.write(b);
    } finally {
      if (cos != null) {
        cos.close();
      }
    }
    byte[] outbytes = bos.toByteArray();
    // verify data were not compressed
    assertTrue("Compressed bytes contrary to configuration(NO_COMPRESSION)",
               outbytes.length >= b.length);
  }

  @Test
  public void testCodecInitWithCompressionLevel() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, true);
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      LOG.info("testCodecInitWithCompressionLevel with native");
      codecTestWithNOCompression(conf,
                            "org.apache.hadoop.io.compress.GzipCodec");
      codecTestWithNOCompression(conf,
                         "org.apache.hadoop.io.compress.DefaultCodec");
    } else {
      LOG.warn("testCodecInitWithCompressionLevel for native skipped"
               + ": native libs not loaded");
    }
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    codecTestWithNOCompression( conf,
                         "org.apache.hadoop.io.compress.DefaultCodec");
  }

  @Test
  public void testCodecPoolCompressorReinit() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, true);
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      GzipCodec gzc = ReflectionUtils.newInstance(GzipCodec.class, conf);
      gzipReinitTest(conf, gzc);
    } else {
      LOG.warn("testCodecPoolCompressorReinit skipped: native libs not loaded");
    }
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    DefaultCodec dfc = ReflectionUtils.newInstance(DefaultCodec.class, conf);
    gzipReinitTest(conf, dfc);
  }

  @Test
  public void testSequenceFileDefaultCodec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 1000000);
  }

  @Test(timeout=20000)
  public void testSequenceFileBZip2Codec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "java-builtin");
    sequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000);
  }

  @Test(timeout=20000)
  public void testSequenceFileBZip2NativeCodec() throws IOException, 
                        ClassNotFoundException, InstantiationException, 
                        IllegalAccessException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "system-native");
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
        sequenceFileCodecTest(conf, 0, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 100);
        sequenceFileCodecTest(conf, 100, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 100);
        sequenceFileCodecTest(conf, 200000, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 
                              1000000);
      } else {
        LOG.warn("Native hadoop library available but native bzip2 is not");
      }
    }
  }

  @Test
  public void testSequenceFileDeflateCodec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DeflateCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DeflateCodec", 1000000);
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
  
  /**
   * Regression test for HADOOP-8423: seeking in a block-compressed
   * stream would not properly reset the block decompressor state.
   */
  @Test
  public void testSnappyMapFile() throws Exception {
    Assume.assumeTrue(SnappyCodec.isNativeCodeLoaded());
    codecTestMapFile(SnappyCodec.class, CompressionType.BLOCK, 100);
  }
  
  private void codecTestMapFile(Class<? extends CompressionCodec> clazz,
      CompressionType type, int records) throws Exception {
    
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Creating MapFiles with " + records  + 
            " records using codec " + clazz.getSimpleName());
    Path path = new Path(new Path(
        System.getProperty("test.build.data", "/tmp")),
      clazz.getSimpleName() + "-" + type + "-" + records);

    LOG.info("Writing " + path);
    createMapFile(conf, fs, path, clazz.newInstance(), type, records);
    MapFile.Reader reader = new MapFile.Reader(path, conf);
    Text key1 = new Text("002");
    assertNotNull(reader.get(key1, new Text()));
    Text key2 = new Text("004");
    assertNotNull(reader.get(key2, new Text()));
  }
  
  private static void createMapFile(Configuration conf, FileSystem fs, Path path, 
      CompressionCodec codec, CompressionType type, int records) throws IOException {
    MapFile.Writer writer = 
        new MapFile.Writer(conf, path,
            MapFile.Writer.keyClass(Text.class),
            MapFile.Writer.valueClass(Text.class),
            MapFile.Writer.compression(type, codec));
    Text key = new Text();
    for (int j = 0; j < records; j++) {
        key.set(String.format("%03d", j));
        writer.append(key, key);
    }
    writer.close();
  }

  public static void main(String[] args) throws IOException {
    int count = 10000;
    String codecClass = "org.apache.hadoop.io.compress.DefaultCodec";

    String usage = "TestCodec [-count N] [-codec <codec class>]";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

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
    // Note that exceptions will propagate out.
    codecTest(conf, seed, count, codecClass);
  }

  @Test
  public void testGzipCompatibility() throws IOException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info("seed: " + seed);

    DataOutputBuffer dflbuf = new DataOutputBuffer();
    GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
    byte[] b = new byte[r.nextInt(128 * 1024 + 1)];
    r.nextBytes(b);
    gzout.write(b);
    gzout.close();

    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(dflbuf.getData(), dflbuf.getLength());

    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
    Decompressor decom = codec.createDecompressor();
    assertNotNull(decom);
    assertEquals(BuiltInGzipDecompressor.class, decom.getClass());
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertArrayEquals(b, dflchk);
  }

  void GzipConcatTest(Configuration conf,
      Class<? extends Decompressor> decomClass) throws IOException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info(decomClass + " seed: " + seed);

    final int CONCAT = r.nextInt(4) + 3;
    final int BUFLEN = 128 * 1024;
    DataOutputBuffer dflbuf = new DataOutputBuffer();
    DataOutputBuffer chkbuf = new DataOutputBuffer();
    byte[] b = new byte[BUFLEN];
    for (int i = 0; i < CONCAT; ++i) {
      GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
      r.nextBytes(b);
      int len = r.nextInt(BUFLEN);
      int off = r.nextInt(BUFLEN - len);
      chkbuf.write(b, off, len);
      gzout.write(b, off, len);
      gzout.close();
    }
    final byte[] chk = Arrays.copyOf(chkbuf.getData(), chkbuf.getLength());

    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
    Decompressor decom = codec.createDecompressor();
    assertNotNull(decom);
    assertEquals(decomClass, decom.getClass());
    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(dflbuf.getData(), dflbuf.getLength());
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertArrayEquals(chk, dflchk);
  }

  @Test
  public void testBuiltInGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    GzipConcatTest(conf, BuiltInGzipDecompressor.class);
  }

  @Test
  public void testNativeGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, true);
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      LOG.warn("skipped: native libs not loaded");
      return;
    }
    GzipConcatTest(conf, GzipCodec.GzipZlibDecompressor.class);
  }

  @Test
  public void testGzipCodecRead() throws IOException {
    // Create a gzipped file and try to read it back, using a decompressor
    // from the CodecPool.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    assertFalse("ZlibFactory is using native libs against request",
        ZlibFactory.isNativeZlibLoaded(conf));

    // Ensure that the CodecPool has a BuiltInZlibInflater in it.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull("zlibDecompressor is null!", zlibDecompressor);
    assertTrue("ZlibFactory returned unexpected inflator",
        zlibDecompressor instanceof BuiltInZlibInflater);
    CodecPool.returnDecompressor(zlibDecompressor);

    // Now create a GZip text file.
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    Path f = new Path(new Path(tmpDir), "testGzipCodecRead.txt.gz");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
      new GZIPOutputStream(new FileOutputStream(f.toString()))));
    final String msg = "This is the message in the file!";
    bw.write(msg);
    bw.close();

    // Now read it back, using the CodecPool to establish the
    // decompressor to use.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream is = fs.open(f);
    is = codec.createInputStream(is, decompressor);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line = br.readLine();
    assertEquals("Didn't get the same message back!", msg, line);
    br.close();
  }

  private void verifyGzipFile(String filename, String msg) throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(filename))));
    try {
      String line = r.readLine();
      assertEquals("Got invalid line back from " + filename, msg, line);
    } finally {
      r.close();
      new File(filename).delete();
    }
  }

  @Test
  public void testGzipLongOverflow() throws IOException {
    LOG.info("testGzipLongOverflow");

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, false);
    assertFalse("ZlibFactory is using native libs against request",
        ZlibFactory.isNativeZlibLoaded(conf));

    // Ensure that the CodecPool has a BuiltInZlibInflater in it.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull("zlibDecompressor is null!", zlibDecompressor);
    assertTrue("ZlibFactory returned unexpected inflator",
        zlibDecompressor instanceof BuiltInZlibInflater);
    CodecPool.returnDecompressor(zlibDecompressor);

    // Now create a GZip text file.
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    Path f = new Path(new Path(tmpDir), "testGzipLongOverflow.bin.gz");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
      new GZIPOutputStream(new FileOutputStream(f.toString()))));

    final int NBUF = 1024 * 4 + 1;
    final char[] buf = new char[1024 * 1024];
    for (int i = 0; i < buf.length; i++) buf[i] = '\0';
    for (int i = 0; i < NBUF; i++) {
      bw.write(buf);
    }
    bw.close();

    // Now read it back, using the CodecPool to establish the
    // decompressor to use.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream is = fs.open(f);
    is = codec.createInputStream(is, decompressor);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    for (int j = 0; j < NBUF; j++) {
      int n = br.read(buf);
      assertEquals("got wrong read length!", n, buf.length);
      for (int i = 0; i < buf.length; i++)
        assertEquals("got wrong byte!", buf[i], '\0');
    }
    br.close();
  }

  public void testGzipCodecWrite(boolean useNative) throws IOException {
    // Create a gzipped file using a compressor from the CodecPool,
    // and try to read it back via the regular GZIPInputStream.

    // Use native libs per the parameter
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, useNative);
    if (useNative) {
      if (!ZlibFactory.isNativeZlibLoaded(conf)) {
        LOG.warn("testGzipCodecWrite skipped: native libs not loaded");
        return;
      }
    } else {
      assertFalse("ZlibFactory is using native libs against request",
          ZlibFactory.isNativeZlibLoaded(conf));
    }

    // Ensure that the CodecPool has a BuiltInZlibDeflater in it.
    Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
    assertNotNull("zlibCompressor is null!", zlibCompressor);
    assertTrue("ZlibFactory returned unexpected deflator",
          useNative ? zlibCompressor instanceof ZlibCompressor
                    : zlibCompressor instanceof BuiltInZlibDeflater);

    CodecPool.returnCompressor(zlibCompressor);

    // Create a GZIP text file via the Compressor interface.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue("Codec for .gz file is not GzipCodec", 
               codec instanceof GzipCodec);

    final String msg = "This is the message we are going to compress.";
    final String tmpDir = System.getProperty("test.build.data", "/tmp/");
    final String fileName = new Path(new Path(tmpDir),
        "testGzipCodecWrite.txt.gz").toString();

    BufferedWriter w = null;
    Compressor gzipCompressor = CodecPool.getCompressor(codec);
    if (null != gzipCompressor) {
      // If it gives us back a Compressor, we should be able to use this
      // to write files we can then read back with Java's gzip tools.
      OutputStream os = new CompressorStream(new FileOutputStream(fileName),
          gzipCompressor);
      w = new BufferedWriter(new OutputStreamWriter(os));
      w.write(msg);
      w.close();
      CodecPool.returnCompressor(gzipCompressor);

      verifyGzipFile(fileName, msg);
    }

    // Create a gzip text file via codec.getOutputStream().
    w = new BufferedWriter(new OutputStreamWriter(
        codec.createOutputStream(new FileOutputStream(fileName))));
    w.write(msg);
    w.close();

    verifyGzipFile(fileName, msg);
  }

  @Test
  public void testGzipCodecWriteJava() throws IOException {
    testGzipCodecWrite(false);
  }

  @Test
  public void testGzipNativeCodecWrite() throws IOException {
    testGzipCodecWrite(true);
  }

  public void testCodecPoolAndGzipDecompressor() {
    // BuiltInZlibInflater should not be used as the GzipCodec decompressor.
    // Assert that this is the case.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", false);
    assertFalse("ZlibFactory is using native libs against request",
                ZlibFactory.isNativeZlibLoaded(conf));

    // This should give us a BuiltInZlibInflater.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull("zlibDecompressor is null!", zlibDecompressor);
    assertTrue("ZlibFactory returned unexpected inflator",
	       zlibDecompressor instanceof BuiltInZlibInflater);
    // its createOutputStream() just wraps the existing stream in a
    // java.util.zip.GZIPOutputStream.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue("Codec for .gz file is not GzipCodec", 
	       codec instanceof GzipCodec);

    // make sure we don't get a null decompressor
    Decompressor codecDecompressor = codec.createDecompressor();
    if (null == codecDecompressor) {
      fail("Got null codecDecompressor");
    }

    // Asking the CodecPool for a decompressor for GzipCodec
    // should not return null
    Decompressor poolDecompressor = CodecPool.getDecompressor(codec);
    if (null == poolDecompressor) {
      fail("Got null poolDecompressor");
    }
    // return a couple decompressors
    CodecPool.returnDecompressor(zlibDecompressor);
    CodecPool.returnDecompressor(poolDecompressor);
    Decompressor poolDecompressor2 = CodecPool.getDecompressor(codec);
    if (poolDecompressor.getClass() == BuiltInGzipDecompressor.class) {
      if (poolDecompressor == poolDecompressor2) {
        fail("Reused java gzip decompressor in pool");
      }
    } else {
      if (poolDecompressor != poolDecompressor2) {
        fail("Did not reuse native gzip decompressor in pool");
      }
    }
  }
}
