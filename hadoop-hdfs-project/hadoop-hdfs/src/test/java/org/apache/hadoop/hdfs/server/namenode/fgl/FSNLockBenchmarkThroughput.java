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

package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class benchmarks the throughput of NN for both global-lock and fine-grained lock.
 */
public class FSNLockBenchmarkThroughput {

  private final int readWriteRatio;
  private final int testingCount;
  private final ExecutorService executorService;
  private final FileSystem fileSystem;

  public FSNLockBenchmarkThroughput(FileSystem fileSystem,
      int readWriteRatio, int testingCount, int concurrency) {
    this.fileSystem = fileSystem;
    this.readWriteRatio = readWriteRatio;
    this.testingCount = testingCount;
    this.executorService = Executors.newFixedThreadPool(concurrency);
  }

  public void benchmark(String lockName) throws Exception {
    System.out.println("Do benchmark for " + lockName);
    Path basePath = new Path("/tmp/fsnlock/benchmark/throughput");

    // private final
    ArrayList<Path> readingPaths = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      Path path = new Path(basePath, "reading_" + i);
      internalWriteFile(this.fileSystem, path, true, null);
      readingPaths.add(path);
    }

    HashMap<String, Integer> detailInfo = new HashMap<>();

    // building tasks.
    List<Callable<Void>> tasks = buildTasks(this.fileSystem,
        basePath, readingPaths, detailInfo);
    Collections.shuffle(tasks);

    long startTime = System.currentTimeMillis();

    // Submit all tasks.
    List<Future<Void>> futures = this.executorService.invokeAll(tasks);

    // Waiting result
    for (Future<Void> f : futures) {
      f.get();
    }

    long endTime = System.currentTimeMillis();

    // Print result.
    System.out.println("The Benchmark result for " + lockName + " is: " + tasks.size()
        + " tasks with readWriteRatio " + this.readWriteRatio + " completed, taking "
        + (endTime - startTime) + "(ms)");
    detailInfo.forEach((k,v) -> System.out.println("\t operationName:" + k + ", testCount:" + v));

    for (Path path : readingPaths) {
      this.fileSystem.delete(path, false);
    }
  }

  // Write a little data to the path.
  private void internalWriteFile(FileSystem fs, Path path, boolean writeData,
      HashMap<String, Integer> detailInfo) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(path)) {
      incOp(detailInfo, "create");
      incOp(detailInfo, "complete");
      byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      if (writeData) {
        outputStream.write(data);
        incOp(detailInfo, "addBlock");
      }
    }
  }

  // Append a little data to the path
  private void internalAppendFile(FileSystem fs, Path path, boolean writeData,
      HashMap<String, Integer> detailInfo) throws IOException {
    try (FSDataOutputStream outputStream = fs.append(path)) {
      incOp(detailInfo, "append");
      incOp(detailInfo, "complete");
      byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      if (writeData) {
        outputStream.write(data);
      }
    }
  }

  /**
   * Include getListing.
   */
  private Callable<Void> getListing(FileSystem fs, Path path,
      HashMap<String, Integer> detailInfo) {
    return () -> {
      fs.listStatus(path);
      incOp(detailInfo, "getListing");
      return null;
    };
  }

  private synchronized void incOp(HashMap<String, Integer> detailInfo, String opName) {
    if (detailInfo != null) {
      int value = detailInfo.getOrDefault(opName, 0);
      detailInfo.put(opName, value + 1);
    }
  }

  /**
   * Include getBlockLocation.
   */
  private Callable<Void> getBlockLocation(FileSystem fs, Path path,
      HashMap<String, Integer> detailInfo) {
    return () -> {
      fs.getFileBlockLocations(path, 0, Long.MAX_VALUE);
      incOp(detailInfo, "getBlockLocation");
      return null;
    };
  }

  /**
   * Include getFileInfo.
   */
  private Callable<Void> getFileInfo(FileSystem fs, Path path,
      HashMap<String, Integer> detailInfo) {
    return () -> {
      fs.getFileStatus(path);
      incOp(detailInfo, "getFileInfo");
      return null;
    };
  }

  /**
   * Include create, addBlock, complete.
   */
  private Callable<Void> writeAndDeleteFile(FileSystem fs, Path path,
      HashMap<String, Integer> detailInfo) {
    return () -> {
      internalWriteFile(fs, path, false, detailInfo);
      fs.delete(path, false);
      incOp(detailInfo, "delete");
      return null;
    };
  }

  /**
   * Include create, addBlock, complete, append, complete, rename, setPermission, setOwner,
   * setReplication and delete.
   */
  private Callable<Void> otherWriteOperation(FileSystem fs, Path path,
      Path renameTargetPath, HashMap<String, Integer> detailInfo) {
    return () -> {
      // Create one file
      internalWriteFile(fs, path, false, detailInfo);

      // Append some data
      internalAppendFile(fs, path, true, detailInfo);

      // Rename
      fs.rename(path, renameTargetPath);
      incOp(detailInfo, "rename");

      // SetPermission
      fs.setPermission(renameTargetPath, FsPermission.getDirDefault());
      incOp(detailInfo, "setPermission");

      // SetOwner
      fs.setOwner(renameTargetPath, "mock_user", "mock_group");
      incOp(detailInfo, "setOwner");

      // SetReplication
      fs.setReplication(renameTargetPath, (short) 4);
      incOp(detailInfo, "setReplication");

      // Delete
      fs.delete(renameTargetPath, false);
      incOp(detailInfo, "delete");

      return null;
    };
  }

  /**
   * Building some callable tasks according to testingCount and readWriteRatio.
   */
  private List<Callable<Void>> buildTasks(FileSystem fs, Path basePath,
      ArrayList<Path> readingPaths, HashMap<String, Integer> detailInfo) {
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < this.testingCount; i++) {

      // contains 4 * 10 write RPCs.
      for (int j = 0; j < 10; j++) {
        Path path = new Path(basePath, "write_" + i + "_" + j);
        tasks.add(writeAndDeleteFile(fs, path, detailInfo));
      }

      // contains 10 write RPCs.
      Path srcPath = new Path(basePath, "src_" + i + "_" + System.nanoTime());
      Path targetPath = new Path(basePath, "target_" + i + "_" + System.nanoTime());
      tasks.add(otherWriteOperation(fs, srcPath, targetPath, detailInfo));

      // The number of write RPCs is 50.
      for (int j = 0; j < this.readWriteRatio * 50; j++) {
        int opNumber = ThreadLocalRandom.current().nextInt(5);
        int pathIndex = ThreadLocalRandom.current().nextInt(readingPaths.size());
        Path path = readingPaths.get(pathIndex);
        if (opNumber <= 1) {
          tasks.add(getFileInfo(fs, path, detailInfo));
        } else if (opNumber <= 3) {
          tasks.add(getBlockLocation(fs, path, detailInfo));
        } else {
          tasks.add(getListing(fs, path, detailInfo));
        }
      }
    }
    return tasks;
  }

  public static void main(String[] args) throws Exception {
    int readWriteRatio = Integer.parseInt(args[0]);
    int testingCount = Integer.parseInt(args[1]);
    int clientNumber = Integer.parseInt(args[2]);

    Configuration conf = new HdfsConfiguration();

    FileSystem fs = FileSystem.get(conf);
    FSNLockBenchmarkThroughput benchmark = new FSNLockBenchmarkThroughput(
        fs, readWriteRatio, testingCount, clientNumber);
    benchmark.benchmark(null);
  }
}
