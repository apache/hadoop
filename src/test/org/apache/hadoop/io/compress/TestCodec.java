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
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

public class TestCodec extends TestCase {

  private static final Log LOG= LogFactory.getLog(TestCodec.class);

  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();
  
  public void testDefaultCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DefaultCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DefaultCodec");
  }
  
  public void testGzipCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
  }
  
  public void testBZip2Codec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
  }

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
      assertTrue("original and compressed-then-decompressed-output not equal",
                 k1.equals(k2) && v1.equals(v2));
    }
    LOG.info("SUCCESS! Completed checking " + count + " records");
  }

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

  public void testCodecInitWithCompressionLevel() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("io.native.lib.available", true);
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
    conf.setBoolean("io.native.lib.available", false);
    codecTestWithNOCompression( conf,
                         "org.apache.hadoop.io.compress.DefaultCodec");
  }

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
  
  public void testSequenceFileDefaultCodec() throws IOException, ClassNotFoundException, 
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 1000000);
  }
  
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
    conf.setBoolean("hadoop.native.lib", false);
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
    Decompressor decom = codec.createDecompressor();
    assertNotNull(decom);
    assertEquals(BuiltInGzipDecompressor.class, decom.getClass());
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertTrue(java.util.Arrays.equals(b, dflchk));
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
    assertTrue(java.util.Arrays.equals(chk, dflchk));
  }

  public void testBuiltInGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", false);
    GzipConcatTest(conf, BuiltInGzipDecompressor.class);
  }

  public void testNativeGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", true);
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      LOG.warn("skipped: native libs not loaded");
      return;
    }
    GzipConcatTest(conf, GzipCodec.GzipZlibDecompressor.class);
    }

  public TestCodec(String name) {
    super(name);
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

  public void testGzipCodecRead() throws IOException {
    // Create a gzipped file and try to read it back, using a decompressor
    // from the CodecPool.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", false);
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

  public void testGzipCodecWrite() throws IOException {
    // Create a gzipped file using a compressor from the CodecPool,
    // and try to read it back via the regular GZIPInputStream.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", false);
    assertFalse("ZlibFactory is using native libs against request",
        ZlibFactory.isNativeZlibLoaded(conf));

    // Ensure that the CodecPool has a BuiltInZlibDeflater in it.
    Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
    assertNotNull("zlibCompressor is null!", zlibCompressor);
    assertTrue("ZlibFactory returned unexpected deflator",
        zlibCompressor instanceof BuiltInZlibDeflater);
    CodecPool.returnCompressor(zlibCompressor);

    // Create a GZIP text file via the Compressor interface.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue("Codec for .gz file is not GzipCodec", codec instanceof GzipCodec);

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
}
