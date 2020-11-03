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
package org.apache.hadoop.tools.dynamometer.workloadgenerator;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CreateFileMapper continuously creates 1 byte files for the specified duration
 * to increase the number of file objects on the NN.
 * <p>
 * Configuration options available:
 * <ul>
 *   <li>{@value NUM_MAPPERS_KEY} (required): Number of mappers to launch.</li>
 *   <li>{@value DURATION_MIN_KEY} (required): Number of minutes to induce
 *       workload for.</li>
 *   <li>{@value SHOULD_DELETE_KEY} (default: {@value SHOULD_DELETE_DEFAULT}):
 *       If true, delete the files after creating them. This can be useful for
 *       generating constant load without increasing the number of file
 *       objects.</li>
 *   <li>{@value FILE_PARENT_PATH_KEY} (default:
 *       {@value FILE_PARENT_PATH_DEFAULT}): The root directory in which to
 *       create files.</li>
 * </ul>
 */
public class CreateFileMapper
    extends WorkloadMapper<NullWritable, NullWritable, NullWritable,
    NullWritable> {

  public static final String NUM_MAPPERS_KEY = "createfile.num-mappers";
  public static final String DURATION_MIN_KEY = "createfile.duration-min";
  public static final String FILE_PARENT_PATH_KEY =
      "createfile.file-parent-path";
  public static final String FILE_PARENT_PATH_DEFAULT = "/tmp/createFileMapper";
  public static final String SHOULD_DELETE_KEY = "createfile.should-delete";
  public static final boolean SHOULD_DELETE_DEFAULT = false;

  /** Custom {@link org.apache.hadoop.mapreduce.Counter} definitions. */
  public enum CREATEFILECOUNTERS {
    NUMFILESCREATED
  }

  private long startTimestampMs;
  private FileSystem fs;
  private Configuration conf;
  private int taskID;
  private String fileParentPath;
  private boolean shouldDelete;
  private long endTimeStampMs;

  @Override
  public String getDescription() {
    return "This mapper creates 1-byte files for the specified duration.";
  }

  @Override
  public List<String> getConfigDescriptions() {
    return Lists.newArrayList(
        NUM_MAPPERS_KEY + " (required): Number of mappers to launch.",
        DURATION_MIN_KEY
            + " (required): Number of minutes to induce workload for.",
        SHOULD_DELETE_KEY + " (default: " + SHOULD_DELETE_DEFAULT
            + "): If true, delete the files after creating "
            + "them. This can be useful for generating constant load without "
            + "increasing the number of file objects.",
        FILE_PARENT_PATH_KEY + " (default: " + FILE_PARENT_PATH_DEFAULT
            + "): The root directory in which to create files.");
  }

  @Override
  public boolean verifyConfigurations(Configuration confToVerify) {
    return confToVerify.get(NUM_MAPPERS_KEY) != null
        && confToVerify.get(DURATION_MIN_KEY) != null;
  }

  @Override
  public void map(NullWritable key, NullWritable value,
      Mapper.Context mapperContext) throws IOException, InterruptedException {
    taskID = mapperContext.getTaskAttemptID().getTaskID().getId();
    conf = mapperContext.getConfiguration();
    String namenodeURI = conf.get(WorkloadDriver.NN_URI);
    startTimestampMs = conf.getLong(WorkloadDriver.START_TIMESTAMP_MS, -1);
    fileParentPath = conf.get(FILE_PARENT_PATH_KEY, FILE_PARENT_PATH_DEFAULT);
    shouldDelete = conf.getBoolean(SHOULD_DELETE_KEY, SHOULD_DELETE_DEFAULT);
    int durationMin = conf.getInt(DURATION_MIN_KEY, -1);
    if (durationMin < 0) {
      throw new IOException("Duration must be positive; got: " + durationMin);
    }
    endTimeStampMs = startTimestampMs
        + TimeUnit.MILLISECONDS.convert(durationMin, TimeUnit.MINUTES);
    fs = FileSystem.get(URI.create(namenodeURI), conf);
    System.out.println("Start timestamp: " + startTimestampMs);

    long currentEpoch = System.currentTimeMillis();
    long delay = startTimestampMs - currentEpoch;
    if (delay > 0) {
      System.out.println("Sleeping for " + delay + " ms");
      Thread.sleep(delay);
    }

    String mapperSpecifcPathPrefix = fileParentPath + "/mapper" + taskID;
    System.out.println("Mapper path prefix: " + mapperSpecifcPathPrefix);
    long numFilesCreated = 0;
    Path path;
    final byte[] content = {0x0};
    while (System.currentTimeMillis() < endTimeStampMs) {
      path = new Path(mapperSpecifcPathPrefix + "/file" + numFilesCreated);
      OutputStream out = fs.create(path);
      out.write(content);
      out.close();
      numFilesCreated++;
      mapperContext.getCounter(CREATEFILECOUNTERS.NUMFILESCREATED)
          .increment(1L);
      if (numFilesCreated % 1000 == 0) {
        mapperContext.progress();
        System.out.println("Number of files created: " + numFilesCreated);
      }
      if (shouldDelete) {
        fs.delete(path, true);
      }
    }
  }
}
