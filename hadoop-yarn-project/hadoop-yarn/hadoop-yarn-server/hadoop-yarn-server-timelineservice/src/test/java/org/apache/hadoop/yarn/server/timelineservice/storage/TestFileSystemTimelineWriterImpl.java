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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Test;

public class TestFileSystemTimelineWriterImpl {

  /**
   * Unit test for PoC YARN 3264
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
    entity.setModifiedTime(1425016502000L);
    te.addEntity(entity);

    FileSystemTimelineWriterImpl fsi = null;
    try {
      fsi = new FileSystemTimelineWriterImpl();
      fsi.init(new YarnConfiguration());
      fsi.start();
      fsi.write("cluster_id", "user_id", "flow_id", "flow_run_id", "app_id", te);

      String fileName = fsi.getOutputRoot() +
          "/entities/cluster_id/user_id/flow_id/flow_run_id/app_id/" + type +
          "/" + id + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      Path path = Paths.get(fileName);
      File f = new File(fileName);
      assertTrue(f.exists() && !f.isDirectory());
      List<String> data = Files.readAllLines(path, StandardCharsets.UTF_8);
      // ensure there's only one entity + 1 new line
      assertTrue(data.size() == 2);
      String d = data.get(0);
      // confirm the contents same as what was written
      assertEquals(d, TimelineUtils.dumpTimelineRecordtoJSON(entity));

      // delete the directory
      File outputDir = new File(fsi.getOutputRoot());
      FileUtils.deleteDirectory(outputDir);
      assertTrue(!(f.exists()));
    } finally {
      if (fsi != null) {
        fsi.stop();
        FileUtils.deleteDirectory(new File(fsi.getOutputRoot()));
      }
    }
  }
}
