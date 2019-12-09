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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class benchmarks the throughput of client read/write for both replica
 * and Erasure Coding.
 * <p/>
 * Currently 4 operations are supported: read, write, generate and cleanup data.
 * Users should specify an operation, the amount of data in MB for a single
 * client, and which storage policy to use, i.e. EC or replication.
 * Optionally, users can specify the number of clients to launch concurrently.
 * The tool launches 1 thread for each client. Number of client is 1 by default.
 * For reading, users can also specify whether stateful or positional read
 * should be used. Stateful read is chosen by default.
 * <p/>
 * Each client reads and writes different files.
 * For writing, client writes a temporary file at the desired amount, and the
 * file will be cleaned up when the test finishes.
 * For reading, each client tries to read the file specific to itself. And the
 * client simply returns if such file does not exist. Therefore, users should
 * generate the files before testing read. Generating data is essentially the
 * same as writing, except that the files won't be cleared at the end.
 * For example, if the user wants to test reading 1024MB data with 10 clients,
 * he/she should firstly generate 1024MB data with 10 (or more) clients.
 */
public class ErasureCodeBenchmarkThroughput
       extends Configured implements Tool {

  private static final int BUFFER_SIZE_MB = 128;
  private static final String DFS_TMP_DIR = System.getProperty(
      "test.benchmark.data", "/tmp/benchmark/data");
  public static final String REP_DIR = DFS_TMP_DIR + "/replica";
  public static final String EC_DIR = DFS_TMP_DIR + "/ec";
  private static final String REP_FILE_BASE = "rep-file-";
  private static final String EC_FILE_BASE = "ec-file-";
  private static final String TMP_FILE_SUFFIX = ".tmp";
  private static final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private static final byte[] data = new byte[BUFFER_SIZE_MB * 1024 * 1024];

  static {
    Random random = new Random();
    random.nextBytes(data);
  }

  private final FileSystem fs;

  public static ErasureCodingPolicy getEcPolicy() {
    return ecPolicy;
  }

  public ErasureCodeBenchmarkThroughput(FileSystem fs) {
    Preconditions.checkArgument(fs instanceof DistributedFileSystem);
    this.fs = fs;
  }

  enum OpType {
    READ, WRITE, GEN, CLEAN;
  }

  public static String getFilePath(int dataSizeMB, boolean isEc) {
    String parent = isEc ? EC_DIR : REP_DIR;
    String file = isEc ? EC_FILE_BASE : REP_FILE_BASE;
    return parent + "/" + file + dataSizeMB + "MB";
  }

  private static void printUsage(String msg) {
    if (msg != null) {
      System.out.println(msg);
    }
    System.err.println("Usage: ErasureCodeBenchmarkThroughput " +
        "<read|write|gen|clean> <size in MB> " +
        "<ec|rep> [num clients] [stf|pos]\n" +
        "Stateful and positional option is only available for read.");
    System.exit(1);
  }

  private List<Long> doBenchmark(boolean isRead, int dataSizeMB,
      int numClients, boolean isEc, boolean statefulRead, boolean isGen)
      throws Exception {
    CompletionService<Long> cs = new ExecutorCompletionService<Long>(
        Executors.newFixedThreadPool(numClients));
    for (int i = 0; i < numClients; i++) {
      cs.submit(isRead ?
          new ReadCallable(dataSizeMB, isEc, i, statefulRead) :
          new WriteCallable(dataSizeMB, isEc, i, isGen));
    }
    List<Long> results = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; i++) {
      results.add(cs.take().get());
    }
    return results;
  }

  private void setReadThreadPoolSize(int numClients) {
    int numThread = numClients * ecPolicy.getNumDataUnits();
    getConf().setInt(HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_KEY,
        numThread);
  }

  private DecimalFormat getDecimalFormat() {
    return new DecimalFormat("#.##");
  }

  private void benchmark(OpType type, int dataSizeMB,
      int numClients, boolean isEc, boolean statefulRead) throws Exception {
    List<Long> sizes = null;
    StopWatch sw = new StopWatch().start();
    switch (type) {
      case READ:
        sizes = doBenchmark(true, dataSizeMB, numClients, isEc,
            statefulRead, false);
        break;
      case WRITE:
        sizes = doBenchmark(
            false, dataSizeMB, numClients, isEc, statefulRead, false);
        break;
      case GEN:
        sizes = doBenchmark(false, dataSizeMB, numClients, isEc,
            statefulRead, true);
    }
    long elapsedSec = sw.now(TimeUnit.SECONDS);
    double totalDataSizeMB = 0;
    for (Long size : sizes) {
      if (size >= 0) {
        totalDataSizeMB += size.doubleValue() / 1024 / 1024;
      }
    }
    double throughput = totalDataSizeMB / elapsedSec;
    DecimalFormat df = getDecimalFormat();
    System.out.println(type + " " + df.format(totalDataSizeMB) +
        " MB data takes: " + elapsedSec + " s.\nTotal throughput: " +
        df.format(throughput) + " MB/s.");
  }

  private void setUpDir() throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.mkdirs(new Path(DFS_TMP_DIR));
    Path repPath = new Path(REP_DIR);
    Path ecPath = new Path(EC_DIR);
    if (!dfs.exists(repPath)) {
      dfs.mkdirs(repPath);
    } else {
      Preconditions.checkArgument(
          dfs.getClient().getErasureCodingPolicy(repPath.toString()) == null);
    }
    if (!dfs.exists(ecPath)) {
      dfs.mkdirs(ecPath);
      dfs.getClient()
          .setErasureCodingPolicy(ecPath.toString(), ecPolicy.getName());
    } else {
      Preconditions.checkArgument(
          dfs.getClient().
              getErasureCodingPolicy(ecPath.toString()).equals(ecPolicy));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    OpType type = null;
    int dataSizeMB = 0;
    boolean isEc = true;
    int numClients = 1;
    boolean statefulRead = true;
    if (args.length >= 3) {
      if (args[0].equals("read")) {
        type = OpType.READ;
      } else if (args[0].equals("write")) {
        type = OpType.WRITE;
      } else if (args[0].equals("gen")) {
        type = OpType.GEN;
      } else if (args[0].equals("clean")) {
        type = OpType.CLEAN;
      } else {
        printUsage("Unknown operation: " + args[0]);
      }
      try {
        dataSizeMB = Integer.parseInt(args[1]);
        if (dataSizeMB <= 0) {
          printUsage("Invalid data size: " + dataSizeMB);
        }
      } catch (NumberFormatException e) {
        printUsage("Invalid data size: " + e.getMessage());
      }
      isEc = args[2].equals("ec");
      if (!isEc && !args[2].equals("rep")) {
        printUsage("Unknown storage policy: " + args[2]);
      }
    } else {
      printUsage(null);
    }
    if (args.length >= 4 && type != OpType.CLEAN) {
      try {
        numClients = Integer.parseInt(args[3]);
        if (numClients <= 0) {
          printUsage("Invalid num of clients: " + numClients);
        }
      } catch (NumberFormatException e) {
        printUsage("Invalid num of clients: " + e.getMessage());
      }
    }
    if (args.length >= 5 && type == OpType.READ) {
      statefulRead = args[4].equals("stf");
      if (!statefulRead && !args[4].equals("pos")) {
        printUsage("Unknown read mode: " + args[4]);
      }
    }

    setUpDir();
    if (type == OpType.CLEAN) {
      cleanUp(dataSizeMB, isEc);
    } else {
      if (type == OpType.READ && isEc) {
        setReadThreadPoolSize(numClients);
      }
      benchmark(type, dataSizeMB, numClients, isEc, statefulRead);
    }
    return 0;
  }

  private void cleanUp(int dataSizeMB, boolean isEc) throws IOException {
    final String fileName = getFilePath(dataSizeMB, isEc);
    Path path = isEc ? new Path(EC_DIR) : new Path(REP_DIR);
    FileStatus fileStatuses[] = fs.listStatus(path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().contains(fileName);
      }
    });
    for (FileStatus fileStatus : fileStatuses) {
      fs.delete(fileStatus.getPath(), false);
    }
  }

  /**
   * A Callable that returns the number of bytes read/written
   */
  private abstract class CallableBase implements Callable<Long> {
    protected final int dataSizeMB;
    protected final boolean isEc;
    protected final int id;

    public CallableBase(int dataSizeMB, boolean isEc, int id)
        throws IOException {
      this.dataSizeMB = dataSizeMB;
      this.isEc = isEc;
      this.id = id;
    }

    protected String getFilePathForThread() {
      return getFilePath(dataSizeMB, isEc) + "_" + id;
    }
  }

  private class WriteCallable extends CallableBase {
    private final boolean isGen;

    public WriteCallable(int dataSizeMB, boolean isEc, int id, boolean isGen)
        throws IOException {
      super(dataSizeMB, isEc, id);
      this.isGen = isGen;
    }

    private long writeFile(Path path) throws IOException {
      StopWatch sw = new StopWatch().start();
      System.out.println("Writing " + path);
      long dataSize = dataSizeMB * 1024 * 1024L;
      long remaining = dataSize;
      try (FSDataOutputStream outputStream = fs.create(path)) {
        if (!isGen) {
          fs.deleteOnExit(path);
        }
        int toWrite;
        while (remaining > 0) {
          toWrite = (int) Math.min(remaining, data.length);
          outputStream.write(data, 0, toWrite);
          remaining -= toWrite;
        }
        System.out.println("Finished writing " + path + ". Time taken: " +
            sw.now(TimeUnit.SECONDS) + " s.");
        return dataSize - remaining;
      }
    }

    @Override
    public Long call() throws Exception {
      String pathStr = getFilePathForThread();
      if (!isGen) {
        pathStr += TMP_FILE_SUFFIX;
      }
      final Path path = new Path(pathStr);
      if (fs.exists(path)) {
        if (isGen) {
          System.out.println("Data already generated at " + path);
        } else {
          System.out.println("Previous tmp data not cleaned " + path);
        }
        return 0L;
      }
      return writeFile(path);
    }
  }

  private class ReadCallable extends CallableBase {
    private final boolean statefulRead;

    public ReadCallable(int dataSizeMB, boolean isEc, int id,
        boolean statefulRead) throws IOException {
      super(dataSizeMB, isEc, id);
      this.statefulRead = statefulRead;
    }

    private long doStateful(FSDataInputStream inputStream) throws IOException {
      long count = 0;
      long bytesRead;
      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE_MB * 1024 * 1024);
      while (true) {
        bytesRead = inputStream.read(buffer);
        if (bytesRead < 0) {
          break;
        }
        count += bytesRead;
        buffer.clear();
      }
      return count;
    }

    private long doPositional(FSDataInputStream inputStream)
        throws IOException {
      long count = 0;
      long bytesRead;
      byte buf[] = new byte[BUFFER_SIZE_MB * 1024 * 1024];
      while (true) {
        bytesRead = inputStream.read(count, buf, 0, buf.length);
        if (bytesRead < 0) {
          break;
        }
        count += bytesRead;
      }
      return count;
    }

    private long readFile(Path path) throws IOException {
      try (FSDataInputStream inputStream = fs.open(path)) {
        StopWatch sw = new StopWatch().start();
        System.out.println((statefulRead ? "Stateful reading " :
            "Positional reading ") + path);
        long totalRead = statefulRead ? doStateful(inputStream) :
            doPositional(inputStream);
        System.out.println(
            (statefulRead ? "Finished stateful read " :
                "Finished positional read ") + path + ". Time taken: " +
                sw.now(TimeUnit.SECONDS) + " s.");
        return totalRead;
      }
    }

    @Override
    public Long call() throws Exception {
      Path path = new Path(getFilePathForThread());
      if (!fs.exists(path) || fs.isDirectory(path)) {
        System.out.println("File not found at " + path +
            ". Call gen first?");
        return 0L;
      }
      long bytesRead = readFile(path);
      long dataSize = dataSizeMB * 1024 * 1024L;
      Preconditions.checkArgument(bytesRead == dataSize,
          "Specified data size: " + dataSize + ", actually read " + bytesRead);
      return bytesRead;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(conf);
    int res = ToolRunner.run(conf,
        new ErasureCodeBenchmarkThroughput(fs), args);
    System.exit(res);
  }
}
