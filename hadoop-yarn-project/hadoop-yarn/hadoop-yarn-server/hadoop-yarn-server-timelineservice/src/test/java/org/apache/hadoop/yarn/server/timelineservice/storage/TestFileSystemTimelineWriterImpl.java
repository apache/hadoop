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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl.buildEntityTypeSubpath;
import static org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl.escape;

public class TestFileSystemTimelineWriterImpl extends AbstractHadoopTestBase {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestFileSystemTimelineWriterImpl.class);

  public static final String UP = ".." + File.separator;

  @TempDir
  private File tmpFolder;

  /**
   * Unit test for PoC YARN 3264.
   *
   * @throws Exception
   */
  @Test
  void testWriteEntityToFile() throws Exception {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "hello";
    String type = "world";
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    te.addEntity(entity);

    TimelineMetric metric = new TimelineMetric();
    String metricId = "CPU";
    metric.setId(metricId);
    metric.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
    metric.addValue(1425016501000L, 1234567L);

    TimelineEntity entity2 = new TimelineEntity();
    String id2 = "metric";
    String type2 = "app";
    entity2.setId(id2);
    entity2.setType(type2);
    entity2.setCreatedTime(1425016503000L);
    entity2.addMetric(metric);
    te.addEntity(entity2);

    Map<String, TimelineMetric> aggregatedMetrics =
        new HashMap<>();
    aggregatedMetrics.put(metricId, metric);

    try (FileSystemTimelineWriterImpl fsi = new FileSystemTimelineWriterImpl()) {
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.getAbsolutePath();
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          outputRoot);
      fsi.init(conf);
      fsi.start();
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", "flow_name",
              "flow_version", 12345678L, "app_id"),
          te, UserGroupInformation.createRemoteUser("user_id"));

      String fileName = outputRoot + File.separator + "entities" +
          File.separator + "cluster_id" + File.separator + "user_id" +
          File.separator + "flow_name" + File.separator + "flow_version" +
          File.separator + "12345678" + File.separator + "app_id" +
          File.separator + type
          + File.separator + id +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      List<String> data = readFromFile(FileSystem.get(conf), new Path(fileName), 2);
      // ensure there's only one entity + 1 new line
      Assertions.assertThat(data).hasSize(2);
      // confirm the contents same as what was written
      assertRecordMatches(data.get(0), entity);

      // verify aggregated metrics
      String fileName2 = fsi.getOutputRoot() + File.separator + "entities" +
          File.separator + "cluster_id" + File.separator + "user_id" +
          File.separator + "flow_name" + File.separator + "flow_version" +
          File.separator + "12345678" + File.separator + "app_id" +
          File.separator + type2
          + File.separator + id2 +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path2 = new Path(fileName2);
      List<String> data2 = readFromFile(FileSystem.get(conf), path2, 2);
      // ensure there's only one entity + 1 new line
      Assertions.assertThat(data).hasSize(2);
      // confirm the contents same as what was written
      assertRecordMatches(data2.get(0), entity2);
    }
  }

  /**
   * Assert a read in string matches the json value of the entity
   * @param d record
   * @param entity expected
   */
  private static void assertRecordMatches(final String d, final TimelineEntity entity)
      throws IOException {
    Assertions.assertThat(d)
        .isEqualTo(TimelineUtils.dumpTimelineRecordtoJSON(entity));
  }

  @Test
  void testWriteMultipleEntities() throws Exception {
    String id = "appId";
    String type = "app";

    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    te1.addEntity(entity);

    TimelineEntities te2 = new TimelineEntities();
    TimelineEntity entity2 = new TimelineEntity();
    entity2.setId(id);
    entity2.setType(type);
    entity2.setCreatedTime(1425016503000L);
    te2.addEntity(entity2);

    try (FileSystemTimelineWriterImpl fsi = new FileSystemTimelineWriterImpl()) {
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.getAbsolutePath();
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          outputRoot);
      fsi.init(conf);
      fsi.start();
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", "flow_name",
              "flow_version", 12345678L, "app_id"),
          te1, UserGroupInformation.createRemoteUser("user_id"));
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", "flow_name",
              "flow_version", 12345678L, "app_id"),
          te2, UserGroupInformation.createRemoteUser("user_id"));

      String fileName = outputRoot + File.separator + "entities"
          + File.separator + buildEntityTypeSubpath("cluster_id", "user_id",
          "flow_name" ,"flow_version" ,12345678, "app_id", type)
          + File.separator + id
          + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(conf);
      List<String> data = readFromFile(fs, path, 3);
      // confirm the contents same as what was written
      assertRecordMatches(data.get(0), entity);

      // confirm the contents same as what was written
      assertRecordMatches(data.get(1), entity2);
    }
  }

  @Test
  void testWriteEntitiesWithEmptyFlowName() throws Exception {
    String id = "appId";
    String type = "app";

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    te.addEntity(entity);

    try (FileSystemTimelineWriterImpl fsi = new FileSystemTimelineWriterImpl()) {
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.getAbsolutePath();
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          outputRoot);
      fsi.init(conf);
      fsi.start();
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", "",
              "flow_version", 12345678L, "app_id"),
          te, UserGroupInformation.createRemoteUser("user_id"));

      String fileName = outputRoot + File.separator + "entities"
          + File.separator + buildEntityTypeSubpath("cluster_id", "user_id",
          "" ,"flow_version" ,12345678, "app_id", type)
          + File.separator + id
          + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

      List<String> data = readFromFile(FileSystem.get(conf), new Path(fileName), 2);
      // confirm the contents same as what was written
      assertRecordMatches(data.get(0), entity);
    }
  }

  /**
   * Stress test the escaping logic.
   */
  @Test
  void testWriteEntitiesWithEscaping() throws Exception {
    String id = UP + "appid";
    String type = UP + "type";

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    te.addEntity(entity);

    try (FileSystemTimelineWriterImpl fsi = new FileSystemTimelineWriterImpl()) {
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.getAbsolutePath();
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          outputRoot);
      fsi.init(conf);
      fsi.start();
      final String flowName = UP + "flow_name?";
      final String flowVersion = UP + "flow_version/";
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", flowName,
              flowVersion, 12345678L, "app_id"),
          te, UserGroupInformation.createRemoteUser("user_id"));

      String fileName = outputRoot + File.separator + "entities"
          + File.separator + buildEntityTypeSubpath("cluster_id", "user_id",
          flowName, flowVersion,12345678, "app_id", type)
          + File.separator + escape(id, "id")
          + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

      List<String> data = readFromFile(FileSystem.get(conf), new Path(fileName), 2);
      // confirm the contents same as what was written
      assertRecordMatches(data.get(0), entity);
    }
  }

  /**
   * Test escape downgrades file separators and inserts the fallback on a null input.
   */
  @Test
  public void testEscapingAndFallback() throws Throwable {
    Assertions.assertThat(escape("", "fallback"))
        .isEqualTo("fallback");
    Assertions.assertThat(escape(File.separator, "fallback"))
        .isEqualTo("_");
    Assertions.assertThat(escape("?:", ""))
        .isEqualTo("__");
  }

  /**
   * Read a file line by line, logging its name first and verifying it is actually a file.
   * Asserts the number of lines read is as expected.
   * @param fs fs
   * @param path path
   * @param entryCount number of entries expected.
   * @return a possibly empty list of lines
   * @throws IOException IO failure
   */
  private List<String> readFromFile(FileSystem fs, Path path, int entryCount)
          throws IOException {

    LOG.info("Reading file from {}", path);
    assertIsFile(fs, path);
    BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(path)));
    List<String> data = new ArrayList<>();
    String line = br.readLine();
    data.add(line);
    while(line != null) {
      line = br.readLine();
      data.add(line);
    }
    Assertions.assertThat(data).hasSize(entryCount);
    return data;
  }

}
