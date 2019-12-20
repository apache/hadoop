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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFileSystemTimelineWriterImpl {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Unit test for PoC YARN 3264.
   *
   * @throws Exception
   */
  @Test
  public void testWriteEntityToFile() throws Exception {
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
        new HashMap<String, TimelineMetric>();
    aggregatedMetrics.put(metricId, metric);

    FileSystemTimelineWriterImpl fsi = null;
    try {
      fsi = new FileSystemTimelineWriterImpl();
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.newFolder().getAbsolutePath();
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
          File.separator + type + File.separator + id +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(conf);
      assertTrue("Specified path(" + fileName + ") should exist: ",
              fs.exists(path));
      FileStatus fileStatus = fs.getFileStatus(path);
      assertTrue("Specified path should be a file",
              !fileStatus.isDirectory());
      List<String> data = readFromFile(fs, path);
      // ensure there's only one entity + 1 new line
      assertTrue("data size is:" + data.size(), data.size() == 2);
      String d = data.get(0);
      // confirm the contents same as what was written
      assertEquals(d, TimelineUtils.dumpTimelineRecordtoJSON(entity));

      // verify aggregated metrics
      String fileName2 = fsi.getOutputRoot() + File.separator + "entities" +
          File.separator + "cluster_id" + File.separator + "user_id" +
          File.separator + "flow_name" + File.separator + "flow_version" +
          File.separator + "12345678" + File.separator + "app_id" +
          File.separator + type2 + File.separator + id2 +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path2 = new Path(fileName2);
      assertTrue("Specified path(" + fileName + ") should exist: ",
              fs.exists(path2));
      FileStatus fileStatus2 = fs.getFileStatus(path2);
      assertTrue("Specified path should be a file",
              !fileStatus2.isDirectory());
      List<String> data2 = readFromFile(fs, path2);
      // ensure there's only one entity + 1 new line
      assertTrue("data size is:" + data2.size(), data2.size() == 2);
      String metricToString = data2.get(0);
      // confirm the contents same as what was written
      assertEquals(metricToString,
          TimelineUtils.dumpTimelineRecordtoJSON(entity2));
    } finally {
      if (fsi != null) {
        fsi.close();
      }
    }
  }

  @Test
  public void testWriteMultipleEntities() throws Exception {
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

    FileSystemTimelineWriterImpl fsi = null;
    try {
      fsi = new FileSystemTimelineWriterImpl();
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.newFolder().getAbsolutePath();
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

      String fileName = outputRoot + File.separator + "entities" +
          File.separator + "cluster_id" + File.separator + "user_id" +
          File.separator + "flow_name" + File.separator + "flow_version" +
          File.separator + "12345678" + File.separator + "app_id" +
          File.separator + type + File.separator + id +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(conf);
      assertTrue("Specified path(" + fileName + ") should exist: ",
          fs.exists(path));
      FileStatus fileStatus = fs.getFileStatus(path);
      assertTrue("Specified path should be a file",
          !fileStatus.isDirectory());
      List<String> data = readFromFile(fs, path);
      assertTrue("data size is:" + data.size(), data.size() == 3);
      String d = data.get(0);
      // confirm the contents same as what was written
      assertEquals(d, TimelineUtils.dumpTimelineRecordtoJSON(entity));


      String metricToString = data.get(1);
      // confirm the contents same as what was written
      assertEquals(metricToString,
          TimelineUtils.dumpTimelineRecordtoJSON(entity2));
    } finally {
      if (fsi != null) {
        fsi.close();
      }
    }
  }

  @Test
  public void testWriteEntitiesWithEmptyFlowName() throws Exception {
    String id = "appId";
    String type = "app";

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    te.addEntity(entity);

    FileSystemTimelineWriterImpl fsi = null;
    try {
      fsi = new FileSystemTimelineWriterImpl();
      Configuration conf = new YarnConfiguration();
      String outputRoot = tmpFolder.newFolder().getAbsolutePath();
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          outputRoot);
      fsi.init(conf);
      fsi.start();
      fsi.write(
          new TimelineCollectorContext("cluster_id", "user_id", "",
              "flow_version", 12345678L, "app_id"),
          te, UserGroupInformation.createRemoteUser("user_id"));

      String fileName = outputRoot + File.separator + "entities" +
          File.separator + "cluster_id" + File.separator + "user_id" +
          File.separator + "" + File.separator + "flow_version" +
          File.separator + "12345678" + File.separator + "app_id" +
          File.separator + type + File.separator + id +
          FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(conf);
      assertTrue("Specified path(" + fileName + ") should exist: ",
          fs.exists(path));
      FileStatus fileStatus = fs.getFileStatus(path);
      assertTrue("Specified path should be a file",
          !fileStatus.isDirectory());
      List<String> data = readFromFile(fs, path);
      assertTrue("data size is:" + data.size(), data.size() == 2);
      String d = data.get(0);
      // confirm the contents same as what was written
      assertEquals(d, TimelineUtils.dumpTimelineRecordtoJSON(entity));
    } finally {
      if (fsi != null) {
        fsi.close();
      }
    }
  }

  private List<String> readFromFile(FileSystem fs, Path path)
          throws IOException {
    BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(path)));
    List<String> data = new ArrayList<>();
    String line = br.readLine();
    data.add(line);
    while(line != null) {
      line = br.readLine();
      data.add(line);
    }
    return data;
  }
}
