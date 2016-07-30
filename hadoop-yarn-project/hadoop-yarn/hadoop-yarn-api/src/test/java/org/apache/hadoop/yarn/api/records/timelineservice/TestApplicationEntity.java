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

package org.apache.hadoop.yarn.api.records.timelineservice;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Various tests for the ApplicationEntity class.
 *
 */
public class TestApplicationEntity {

  @Test
  public void testIsApplicationEntity() {
    TimelineEntity te = new TimelineEntity();
    te.setType(TimelineEntityType.YARN_APPLICATION.toString());
    assertTrue(ApplicationEntity.isApplicationEntity(te));

    te = null;
    assertEquals(false, ApplicationEntity.isApplicationEntity(te));

    te = new TimelineEntity();
    te.setType(TimelineEntityType.YARN_CLUSTER.toString());
    assertEquals(false, ApplicationEntity.isApplicationEntity(te));
  }

  @Test
  public void testGetApplicationEvent() {
    TimelineEntity te = null;
    TimelineEvent tEvent = ApplicationEntity.getApplicationEvent(te,
        "no event");
    assertEquals(null, tEvent);

    te = new TimelineEntity();
    te.setType(TimelineEntityType.YARN_APPLICATION.toString());
    TimelineEvent event = new TimelineEvent();
    event.setId("start_event");
    event.setTimestamp(System.currentTimeMillis());
    te.addEvent(event);
    tEvent = ApplicationEntity.getApplicationEvent(te, "start_event");
    assertEquals(event, tEvent);

    te = new TimelineEntity();
    te.setType(TimelineEntityType.YARN_CLUSTER.toString());
    event = new TimelineEvent();
    event.setId("start_event_cluster");
    event.setTimestamp(System.currentTimeMillis());
    te.addEvent(event);
    tEvent = ApplicationEntity.getApplicationEvent(te, "start_event_cluster");
    assertEquals(null, tEvent);

  }
}
