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
    System.err.println("Usage: CompressionTest HDFS_PATH none|gz|lzo");
    System.exit(1);
  }

  protected static DistributedFileSystem openConnection(String urlString)
  throws java.net.URISyntaxException, java.io.IOException {
    URI dfsUri = new URI(urlString);
    Configuration dfsConf = new Configuration();
    DistributedFileSystem dfs = new DistributedFileSystem();
    dfs.initialize(dfsUri, dfsConf);
    return dfs;
  }

  protected static boolean closeConnection(DistributedFileSystem dfs) {
    if (dfs != null) {
      try {
        dfs.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return dfs == null;
  }

  public static void main(String[] args) {
    if (args.length != 2) usage();
    try {
      DistributedFileSystem dfs = openConnection(args[0]);
      dfs.delete(path, false);
      HFile.Writer writer = new HFile.Writer(dfs, path,
        HFile.DEFAULT_BLOCKSIZE, args[1], null);
      writer.append(Bytes.toBytes("testkey"), Bytes.toBytes("testval"));
      writer.appendFileInfo(Bytes.toBytes("infokey"), Bytes.toBytes("infoval"));
      writer.close();

      HFile.Reader reader = new HFile.Reader(dfs, path, null, false);
      reader.loadFileInfo();
      byte[] key = reader.getFirstKey();
      boolean rc = Bytes.toString(key).equals("testkey");
      reader.close();

      dfs.delete(path, false);
      closeConnection(dfs);

      if (rc) System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("FAILED");
    System.exit(1);
  }
}
