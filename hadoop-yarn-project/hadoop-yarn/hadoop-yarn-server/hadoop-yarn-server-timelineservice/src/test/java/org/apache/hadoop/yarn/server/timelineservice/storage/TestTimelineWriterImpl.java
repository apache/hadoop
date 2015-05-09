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

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

public class TestTimelineWriterImpl {
  static TimelineEntities getStandardTestTimelineEntities(int listSize) {
    TimelineEntities te = new TimelineEntities();
    for (int i = 0; i < listSize; i++) {
      TimelineEntity entity = new TimelineEntity();
      String id = "hello" + i;
      String type = "testType";
      entity.setId(id);
      entity.setType(type);
      entity.setCreatedTime(1425016501000L + i);
      entity.setModifiedTime(1425016502000L + i);
      if (i > 0) {
        entity.addRelatesToEntity(type, "hello" + i);
        entity.addRelatesToEntity(type, "hello" + (i - 1));
      }
      if (i < listSize - 1) {
        entity.addIsRelatedToEntity(type, "hello" + i);
        entity.addIsRelatedToEntity(type, "hello" + (i + 1));
      }
      int category = i % 4;
      switch (category) {
      case 0:
        entity.addConfig("config", "config" + i);
        // Fall through deliberately
      case 1:
        entity.addInfo("info1", new Integer(i));
        entity.addInfo("info2", "helloworld");
        // Fall through deliberately
      case 2:
        break;
      case 3:
        entity.addConfig("config", "config" + i);
        TimelineEvent event = new TimelineEvent();
        event.setId("test event");
        event.setTimestamp(1425016501100L + i);
        event.addInfo("test_info", "content for " + entity.getId());
        event.addInfo("test_info1", new Integer(i));
        entity.addEvent(event);
        TimelineMetric metric = new TimelineMetric();
        metric.setId("HDFS_BYTES_READ");
        metric.addValue(1425016501100L + i, 8000 + i);
        entity.addMetric(metric);
        break;
      }
      te.addEntity(entity);
    }
    return te;
  }
}
