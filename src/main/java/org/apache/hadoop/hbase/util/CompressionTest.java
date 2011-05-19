/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.net.URI;

/**
 * Compression validation test.  Checks compression is working.  Be sure to run
 * on every node in your cluster.
 */
public class CompressionTest {
  static final Log LOG = LogFactory.getLog(CompressionTest.class);

  public static boolean testCompression(String codec) {
    codec = codec.toLowerCase();

    Compression.Algorithm a;

    try {
      a = Compression.getCompressionAlgorithmByName(codec);
    } catch (IllegalArgumentException e) {
      LOG.warn("Codec type: " + codec + " is not known");
      return false;
    }

    try {
      testCompression(a);
      return true;
    } catch (IOException ignored) {
      LOG.warn("Can't instantiate codec: " + codec, ignored);
      return false;
    }
  }

  private final static Boolean[] compressionTestResults
      = new Boolean[Compression.Algorithm.values().length];
  static {
    for (int i = 0 ; i < compressionTestResults.length ; ++i) {
      compressionTestResults[i] = null;
    }
  }

  public static void testCompression(Compression.Algorithm algo)
      throws IOException {
    if (compressionTestResults[algo.ordinal()] != null) {
      if (compressionTestResults[algo.ordinal()]) {
        return ; // already passed test, dont do it again.
      } else {
        // failed.
        throw new IOException("Compression algorithm '" + algo.getName() + "'" +
        " previously failed test.");
      }
    }

    try {
      Compressor c = algo.getCompressor();
      algo.returnCompressor(c);
      compressionTestResults[algo.ordinal()] = true; // passes
    } catch (Throwable t) {
      compressionTestResults[algo.ordinal()] = false; // failure
      throw new IOException(t);
    }
  }

  protected static Path path = new Path(".hfile-comp-test");

  public static void usage() {
    System.err.println(
      "Usage: CompressionTest <path> none|gz|lzo|snappy\n" +
      "\n" +
      "For example:\n" +
      "  hbase " + CompressionTest.class + " file:///tmp/testfile gz\n");
    System.exit(1);
  }

  public static void doSmokeTest(FileSystem fs, Path path, String codec)
  throws Exception {
    HFile.Writer writer = new HFile.Writer(
      fs, path, HFile.DEFAULT_BLOCKSIZE, codec, null);
    writer.append(Bytes.toBytes("testkey"), Bytes.toBytes("testval"));
    writer.appendFileInfo(Bytes.toBytes("infokey"), Bytes.toBytes("infoval"));
    writer.close();

    HFile.Reader reader = new HFile.Reader(fs, path, null, false, false);
    reader.loadFileInfo();
    byte[] key = reader.getFirstKey();
    boolean rc = Bytes.toString(key).equals("testkey");
    reader.close();

    if (!rc) {
      throw new Exception("Read back incorrect result: " +
                          Bytes.toStringBinary(key));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Path path = new Path(args[0]);
    FileSystem fs = path.getFileSystem(conf);
    try {
      doSmokeTest(fs, path, args[1]);
    } finally {
      fs.delete(path, false);
    }
    System.out.println("SUCCESS");
  }
}
