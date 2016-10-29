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

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/** Main class for Timeline Reader. */
@Private
@Unstable
public class TimelineReaderServer extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TimelineReaderServer.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  static final String TIMELINE_READER_MANAGER_ATTR =
      "timeline.reader.manager";

  private HttpServer2 readerWebServer;
  private TimelineReaderManager timelineReaderManager;

  public TimelineReaderServer() {
    super(TimelineReaderServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new YarnException("timeline service v.2 is not enabled");
    }

    TimelineReader timelineReaderStore = createTimelineReaderStore(conf);
    addService(timelineReaderStore);
    timelineReaderManager = createTimelineReaderManager(timelineReaderStore);
    addService(timelineReaderManager);
    super.serviceInit(conf);
  }

  private TimelineReader createTimelineReaderStore(Configuration conf) {
    TimelineReader readerStore = ReflectionUtils.newInstance(conf.getClass(
        YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        HBaseTimelineReaderImpl.class, TimelineReader.class), conf);
    LOG.info("Using store " + readerStore.getClass().getName());
    readerStore.init(conf);
    return readerStore;
  }

  private TimelineReaderManager createTimelineReaderManager(
      TimelineReader timelineReaderStore) {
    return new TimelineReaderManager(timelineReaderStore);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    startTimelineReaderWebApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (readerWebServer != null) {
      readerWebServer.stop();
    }
    super.serviceStop();
  }

  private void startTimelineReaderWebApp() {
    Configuration conf = getConfig();
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
        YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        WebAppUtils.getTimelineReaderWebAppURL(conf));
    LOG.info("Instantiating TimelineReaderWebApp at " + bindAddress);
    try {
      HttpServer2.Builder builder = new HttpServer2.Builder()
            .setName("timeline")
            .setConf(conf)
            .addEndpoint(URI.create("http://" + bindAddress));
      readerWebServer = builder.build();

      setupOptions(conf);

      readerWebServer.addJerseyResourcePackage(
          TimelineReaderWebServices.class.getPackage().getName() + ";"
              + GenericExceptionHandler.class.getPackage().getName() + ";"
              + YarnJacksonJaxbJsonProvider.class.getPackage().getName(),
          "/*");
      readerWebServer.setAttribute(TIMELINE_READER_MANAGER_ATTR,
          timelineReaderManager);
      readerWebServer.start();
    } catch (Exception e) {
      String msg = "TimelineReaderWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  /**
   * Sets up some options and filters.
   *
   * @param conf Configuration
   */
  protected void setupOptions(Configuration conf) {
    Map<String, String> options = new HashMap<>();
    String username = conf.get(HADOOP_HTTP_STATIC_USER,
        DEFAULT_HADOOP_HTTP_STATIC_USER);
    options.put(HADOOP_HTTP_STATIC_USER, username);
    HttpServer2.defineFilter(readerWebServer.getWebAppContext(),
        "static_user_filter_timeline",
        StaticUserWebFilter.StaticUserFilter.class.getName(),
        options, new String[] {"/*"});
  }

  @VisibleForTesting
  int getWebServerPort() {
    return readerWebServer.getConnectorAddress(0).getPort();
  }

  static TimelineReaderServer startTimelineReaderServer(String[] args,
      Configuration conf) {
    Thread.setDefaultUncaughtExceptionHandler(
        new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(TimelineReaderServer.class,
        args, LOG);
    TimelineReaderServer timelineReaderServer = null;
    try {
      timelineReaderServer = new TimelineReaderServer();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(timelineReaderServer),
          SHUTDOWN_HOOK_PRIORITY);
      timelineReaderServer.init(conf);
      timelineReaderServer.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting TimelineReaderWebServer", t);
      ExitUtil.terminate(-1, "Error starting TimelineReaderWebServer");
    }
    return timelineReaderServer;
  }

  public static void main(String[] args) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    startTimelineReaderServer(args, conf);
  }
}