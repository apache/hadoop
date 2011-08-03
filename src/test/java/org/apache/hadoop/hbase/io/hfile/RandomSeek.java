/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Random seek test.
 */
public class RandomSeek {
  private static List<String> slurp(String fname) throws IOException {
    BufferedReader istream = new BufferedReader(new FileReader(fname));
    String str;
    List<String> l = new ArrayList<String>();
    while ( (str=istream.readLine()) != null) {
      String [] parts = str.split(",");
      l.add(parts[0] + ":" + parts[1] + ":" + parts[2]);
    }
    istream.close();
    return l;
  }

  private static String randKey(List<String> keys) {
    Random r = new Random();
    //return keys.get(r.nextInt(keys.size()));
    return "2" + Integer.toString(7+r.nextInt(2)) + Integer.toString(r.nextInt(100));
    //return new String(r.nextInt(100));
  }

  public static void main(String [] argv) throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 64*1024);
    RawLocalFileSystem rlfs = new RawLocalFileSystem();
    rlfs.setConf(conf);
    LocalFileSystem lfs = new LocalFileSystem(rlfs);

    Path path = new Path("/Users/ryan/rfile.big.txt");
    long start = System.currentTimeMillis();
    SimpleBlockCache cache = new SimpleBlockCache();
    Reader reader = HFile.createReader(lfs, path, cache, false, false);
    reader.loadFileInfo();
    System.out.println(reader.getTrailer());
    long end = System.currentTimeMillis();

    System.out.println("Index read time: " + (end - start));

    List<String> keys = slurp("/Users/ryan/xaa.50k");

    // Get a scanner that doesn't cache and that uses pread.
    HFileScanner scanner = reader.getScanner(false, true);
    int count;
    long totalBytes = 0;
    int notFound = 0;

    start = System.nanoTime();
    for(count = 0; count < 500000; ++count) {
      String key = randKey(keys);
      byte [] bkey = Bytes.toBytes(key);
      int res = scanner.seekTo(bkey);
      if (res == 0) {
        ByteBuffer k = scanner.getKey();
        ByteBuffer v = scanner.getValue();
        totalBytes += k.limit();
        totalBytes += v.limit();
      } else {
        ++ notFound;
      }
      if (res == -1) {
        scanner.seekTo();
      }
      // Scan for another 1000 rows.
      for (int i = 0; i < 1000; ++i) {
        if (!scanner.next())
          break;
        ByteBuffer k = scanner.getKey();
        ByteBuffer v = scanner.getValue();
        totalBytes += k.limit();
        totalBytes += v.limit();
      }

      if ( count % 1000 == 0 ) {
        end = System.nanoTime();

            System.out.println("Cache block count: " + cache.size() + " dumped: "+ cache.dumps);
            //System.out.println("Cache size: " + cache.heapSize());
            double msTime = ((end - start) / 1000000.0);
            System.out.println("Seeked: "+ count + " in " + msTime + " (ms) "
                + (1000.0 / msTime ) + " seeks/ms "
                + (msTime / 1000.0) + " ms/seek");

            start = System.nanoTime();
      }
    }
    System.out.println("Total bytes: " + totalBytes + " not found: " + notFound);
  }
}
