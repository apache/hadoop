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

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestTimelineReaderServer {

  @Test
  @Timeout(60000)
  void testStartStopServer() throws Exception {
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

  /**
   * A web server that fails to stop must not take the child services down with
   * it: the reader is one of them, and it owns the storage monitor's polling
   * executor, whose threads are not daemons.
   */
  @Test
  @Timeout(60)
  void testChildServicesStoppedWhenWebAppStopFails() throws Exception {
    final IOException webAppFailure =
        new IOException("simulated web server stop failure");

    @SuppressWarnings("resource")
    TimelineReaderServer server = new TimelineReaderServer() {
      @Override
      void stopTimelineReaderWebApp() throws Exception {
        super.stopTimelineReaderWebApp();
        throw webAppFailure;
      }
    };
    try {
      server.init(createStartStopConfig());
      server.start();
      List<Service> children = server.getServices();
      assertEquals(2, children.size());

      ServiceStateException thrown =
          assertThrows(ServiceStateException.class, server::stop);
      assertSame(webAppFailure, thrown.getCause());

      for (Service child : children) {
        assertEquals(STATE.STOPPED, child.getServiceState(),
            child.getName() + " was left running by the failed stop");
      }
    } finally {
      // stop() here would rethrow the simulated failure and mask any earlier
      // one; it is a no-op anyway once the service has reached STOPPED.
      ServiceOperations.stopQuietly(server);
    }
  }

  /**
   * When both the web server and a child service fail to stop, the web
   * server's failure is the one reported, with the child's attached as
   * suppressed, and the remaining child services are still stopped.
   */
  @Test
  @Timeout(60)
  void testWebAppStopFailureKeptWhenChildStopAlsoFails() throws Exception {
    final IOException webAppFailure =
        new IOException("simulated web server stop failure");
    final IllegalStateException childFailure =
        new IllegalStateException("simulated child service stop failure");
    final Service failingChild = new AbstractService("FailingChild") {
      @Override
      protected void serviceStop() throws Exception {
        throw childFailure;
      }
    };

    @SuppressWarnings("resource")
    TimelineReaderServer server = new TimelineReaderServer() {
      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        // Added last, so it is stopped first: the reader and the manager
        // must still be stopped after it fails.
        failingChild.init(conf);
        addService(failingChild);
      }

      @Override
      void stopTimelineReaderWebApp() throws Exception {
        super.stopTimelineReaderWebApp();
        throw webAppFailure;
      }
    };
    try {
      server.init(createStartStopConfig());
      server.start();
      List<Service> children = server.getServices();
      assertEquals(3, children.size());

      ServiceStateException thrown =
          assertThrows(ServiceStateException.class, server::stop);
      assertSame(webAppFailure, thrown.getCause());
      Throwable[] suppressed = webAppFailure.getSuppressed();
      assertEquals(1, suppressed.length);
      assertSame(childFailure, suppressed[0]);

      for (Service child : children) {
        assertEquals(STATE.STOPPED, child.getServiceState(),
            child.getName() + " was left running by the failed stop");
      }
    } finally {
      ServiceOperations.stopQuietly(server);
    }
  }

  @Test
  @Timeout(60000)
  void testTimelineReaderServerWithInvalidTimelineReader() {
    assertThrows(YarnRuntimeException.class, () -> {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
          "localhost:0");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
          Object.class.getName());
      runTimelineReaderServerWithConfig(conf);
    });
  }

  @Test
  @Timeout(60000)
  void testTimelineReaderServerWithNonexistentTimelineReader() {
    assertThrows(YarnRuntimeException.class, () -> {
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
    });
  }

  /**
   * @return a configuration for a server that starts and stops cleanly,
   *     backed by the file system timeline reader
   */
  private static Configuration createStartStopConfig() {
    Configuration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
        "localhost:0");
    config.setClass(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        FileSystemTimelineReaderImpl.class, TimelineReader.class);
    return config;
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
