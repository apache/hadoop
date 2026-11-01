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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Example: how a client turns on and uses sequential read-ahead prefetch.
 *
 * <p>Prefetch is configured entirely through {@link Configuration} string keys
 * (no new public API) and is transparent — the application reads the file with
 * the normal {@code FileSystem.open(...).read(...)} loop and the bytes are
 * served from the in-memory read-ahead cache when available, otherwise read
 * directly from the DataNode. The feature is off by default and only engages
 * for files with more than one block.
 *
 * <p>Run against a real cluster:
 * <pre>
 *   hadoop org.apache.hadoop.hdfs.PrefetchReadExample hdfs://nn:8020/path/to/bigfile
 * </pre>
 */
public final class PrefetchReadExample {

  private PrefetchReadExample() {
  }

  /**
   * Build a Configuration with read-ahead prefetch enabled and tuned. Only
   * {@code dfs.client.prefetch.enabled} is required; the rest have sensible
   * defaults and are shown here for documentation.
   */
  public static Configuration prefetchConfig() {
    Configuration conf = new Configuration();

    // Required: turn the feature on for this client / FileSystem instance.
    conf.setBoolean("dfs.client.prefetch.enabled", true);

    // Optional tuning (defaults shown):
    // Total prefetch memory per stream; split into N = size / blockSize
    // per-block buffers. The main memory/parallelism knob.
    conf.setLong("dfs.client.prefetch.size", 512L * 1024 * 1024);   // 512 MB
    // Max blocks a single stream fetches concurrently.
    conf.setInt("dfs.client.prefetch.threads", 4);
    // Granularity of incremental fill / readiness (and cancel checkpoint).
    conf.setInt("dfs.client.prefetch.chunk.size", 8 * 1024 * 1024); // 8 MB
    // Shared (JVM-wide) prefetch worker pool size; <= 0 disables prefetch.
    conf.setInt("dfs.client.prefetch.threadpool.size", 16);
    // Global cap on prefetch bytes across all streams of this client.
    conf.setLong("dfs.client.prefetch.max.bytes", 2L * 1024 * 1024 * 1024);

    return conf;
  }

  /**
   * Read {@code path} sequentially with prefetch enabled and return the number
   * of bytes read. The read loop is exactly the same as a normal HDFS read —
   * prefetch is transparent.
   */
  public static long readWithPrefetch(URI fsUri, Path path) throws IOException {
    Configuration conf = prefetchConfig();
    long total = 0;
    byte[] buffer = new byte[1024 * 1024]; // 1 MB application buffer
    try (FileSystem fs = FileSystem.get(fsUri, conf);
         FSDataInputStream in = fs.open(path)) {
      int n;
      while ((n = in.read(buffer, 0, buffer.length)) > 0) {
        total += n;
      }
    }
    return total;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println(
          "Usage: PrefetchReadExample <hdfs-uri-path>   e.g. hdfs://nn:8020/data/big");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    URI fsUri = path.toUri();

    long start = System.currentTimeMillis();
    long bytes = readWithPrefetch(fsUri, path);
    long ms = System.currentTimeMillis() - start;

    System.out.printf("Read %d bytes from %s in %d ms (%.1f MB/s) with prefetch%n",
        bytes, path, ms, (bytes / (1024.0 * 1024.0)) / Math.max(1, ms / 1000.0));
  }
}
