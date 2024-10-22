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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineEntitySetWriter;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineEntityWriter;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineHealthWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.reader.security.TimelineReaderAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timelineservice.reader.security.TimelineReaderWhitelistAuthorizationFilterInitializer;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.server.webapp.LogWebService;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main class for Timeline Reader. */
@Private
@Unstable
public class TimelineReaderServer extends CompositeService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderServer.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  static final String TIMELINE_READER_MANAGER_ATTR =
      "timeline.reader.manager";

  private HttpServer2 readerWebServer;
  private TimelineReaderManager timelineReaderManager;
  private String webAppURLWithoutScheme;


  public TimelineReaderServer() {
    super(TimelineReaderServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new YarnException("timeline service v.2 is not enabled");
    }
    webAppURLWithoutScheme =
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(conf);
    InetSocketAddress bindAddr =
        NetUtils.createSocketAddr(webAppURLWithoutScheme);
    // Login from keytab if security is enabled.
    try {
      SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
          YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, bindAddr.getHostName());
    } catch(IOException e) {
      throw new YarnRuntimeException("Failed to login from keytab", e);
    }

    TimelineReader timelineReaderStore = createTimelineReaderStore(conf);
    timelineReaderStore.init(conf);
    addService(timelineReaderStore);
    timelineReaderManager = createTimelineReaderManager(timelineReaderStore);
    addService(timelineReaderManager);
    super.serviceInit(conf);
  }

  private TimelineReader createTimelineReaderStore(final Configuration conf) {
    String timelineReaderClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READER_CLASS);
    LOG.info("Using store: {}", timelineReaderClassName);
    try {
      Class<?> timelineReaderClazz = Class.forName(timelineReaderClassName);
      if (TimelineReader.class.isAssignableFrom(timelineReaderClazz)) {
        return (TimelineReader) ReflectionUtils.newInstance(
            timelineReaderClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + timelineReaderClassName
            + " not instance of " + TimelineReader.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineReader: "
          + timelineReaderClassName, e);
    }
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

  private void join() {
    // keep the main thread that started the server up until it receives a stop
    // signal
    if (readerWebServer != null) {
      try {
        readerWebServer.join();
      } catch (InterruptedException ignore) {}
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (readerWebServer != null) {
      readerWebServer.stop();
    }
    super.serviceStop();
  }

  protected void addFilters(Configuration conf) {
    boolean enableCorsFilter = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED,
        YarnConfiguration.TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED_DEFAULT);
    // Setup CORS
    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<String>();
    if (!initializers.contains(
        ProxyUserAuthenticationFilterInitializer.class.getName())) {
      if (!initializers.contains(
          TimelineReaderAuthenticationFilterInitializer.class.getName())) {
        defaultInitializers.add(
            TimelineReaderAuthenticationFilterInitializer.class.getName());
      }
    } else {
      defaultInitializers.add(
          ProxyUserAuthenticationFilterInitializer.class.getName());
    }

    defaultInitializers.add(
        TimelineReaderWhitelistAuthorizationFilterInitializer.class.getName());

    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);
  }

  private void startTimelineReaderWebApp() {
    Configuration conf = getConfig();
    addFilters(conf);

    String hostProperty = YarnConfiguration.TIMELINE_SERVICE_READER_BIND_HOST;
    String host = conf.getTrimmed(hostProperty);
    if (host == null || host.isEmpty()) {
      // if reader bind-host is not set, fall back to timeline-service.bind-host
      // to maintain compatibility
      hostProperty = YarnConfiguration.TIMELINE_SERVICE_BIND_HOST;
    }
    String bindAddress = WebAppUtils
        .getWebAppBindURL(conf, hostProperty, webAppURLWithoutScheme);

    LOG.info("Instantiating TimelineReaderWebApp at {}", bindAddress);
    try {

      String httpScheme = WebAppUtils.getHttpSchemePrefix(conf);

      HttpServer2.Builder builder = new HttpServer2.Builder()
            .setName("timeline")
            .setConf(conf)
            .addEndpoint(URI.create(httpScheme + bindAddress));

      if (httpScheme.equals(WebAppUtils.HTTPS_PREFIX)) {
        WebAppUtils.loadSslConfiguration(builder, conf);
      }
      readerWebServer = builder.build();
      readerWebServer.addJerseyResourceConfig(configure(), "/*", null);
      readerWebServer.setAttribute(TIMELINE_READER_MANAGER_ATTR,
          timelineReaderManager);
      readerWebServer.start();
    } catch (Exception e) {
      String msg = "TimelineReaderWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  @VisibleForTesting
  public int getWebServerPort() {
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
      LOG.error("Error starting TimelineReaderWebServer", t);
      ExitUtil.terminate(-1, "Error starting TimelineReaderWebServer");
    }
    return timelineReaderServer;
  }

  protected static ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.hadoop.yarn.server.timelineservice.reader");
    config.packages("org.apache.hadoop.yarn.api.records.writer");
    config.register(LogWebService.class);
    config.register(GenericExceptionHandler.class);
    config.register(TimelineReaderWebServices.class);
    config.register(TimelineEntitySetWriter.class);
    config.register(TimelineEntityWriter.class);
    config.register(TimelineHealthWriter.class);
    config.register(new JettisonFeature()).register(YarnJacksonJaxbJsonProvider.class);
    return config;
  }

  public static void main(String[] args) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, 2.0f);
    TimelineReaderServer server = startTimelineReaderServer(args, conf);
    server.join();
  }
}