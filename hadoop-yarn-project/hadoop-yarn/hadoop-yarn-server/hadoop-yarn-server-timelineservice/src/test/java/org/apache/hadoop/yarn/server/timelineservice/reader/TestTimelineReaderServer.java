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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.junit.Test;

public class TestTimelineReaderServer {

  @Test(timeout = 60000)
  public void testStartStopServer() throws Exception {
    @SuppressWarnings("resource")
    TimelineReaderServer server = new TimelineReaderServer();
    Configuration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
        "localhost:0");
    config.setClass(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        FileSystemTimelineReaderImpl.class, TimelineReader.class);
    try {
      server.init(config);
      assertEquals(STATE.INITED, server.getServiceState());
      assertEquals(2, server.getServices().size());

      server.start();
      assertEquals(STATE.STARTED, server.getServiceState());

      server.stop();
      assertEquals(STATE.STOPPED, server.getServiceState());
    } finally {
      server.stop();
    }
  }

  @Test(timeout = 60000, expected = YarnRuntimeException.class)
  public void testTimelineReaderServerWithInvalidTimelineReader() {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
        "localhost:0");
    conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        Object.class.getName());
    runTimelineReaderServerWithConfig(conf);
  }

  @Test(timeout = 60000, expected = YarnRuntimeException.class)
  public void testTimelineReaderServerWithNonexistentTimelineReader() {
    String nonexistentTimelineReaderClass = "org.apache.org.yarn.server." +
        "timelineservice.storage.XXXXXXXX";
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
        "localhost:0");
    conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        nonexistentTimelineReaderClass);
    runTimelineReaderServerWithConfig(conf);
  }

  /**
   * Run a TimelineReaderServer with a given configuration.
   * @param conf configuration to run TimelineReaderServer with
   */
  private static void runTimelineReaderServerWithConfig(
      final Configuration conf) {
    TimelineReaderServer server = new TimelineReaderServer();
    try {
      server.init(conf);
      server.start();
    } finally {
      server.stop();
    }
  }

}
