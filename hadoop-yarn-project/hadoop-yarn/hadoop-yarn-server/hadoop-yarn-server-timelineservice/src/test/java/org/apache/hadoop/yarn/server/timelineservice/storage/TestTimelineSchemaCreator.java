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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link TimelineSchemaCreator}.
 */
public class TestTimelineSchemaCreator {

  @Test
  void testTimelineSchemaCreation() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS,
        "org.apache.hadoop.yarn.server.timelineservice.storage" +
            ".DummyTimelineSchemaCreator");
    TimelineSchemaCreator timelineSchemaCreator = new TimelineSchemaCreator();
    assertEquals(0, timelineSchemaCreator
        .createTimelineSchema(new String[]{}, conf));
  }
}