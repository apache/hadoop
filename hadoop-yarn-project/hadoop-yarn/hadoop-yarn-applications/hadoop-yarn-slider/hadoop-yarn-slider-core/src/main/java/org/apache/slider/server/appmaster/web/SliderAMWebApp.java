/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.hadoop.yarn.webapp.Dispatcher;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.web.rest.AMWadlGeneratorConfig;
import org.apache.slider.server.appmaster.web.rest.AMWebServices;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;
import org.apache.slider.server.appmaster.web.rest.SliderJacksonJaxbJsonProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 */
public class SliderAMWebApp extends WebApp {
  public static final String BASE_PATH = "slideram";
  public static final String CONTAINER_STATS = "/stats";
  public static final String CLUSTER_SPEC = "/spec";

  private final WebAppApi webAppApi;

  public SliderAMWebApp(WebAppApi webAppApi) {
    Preconditions.checkArgument(webAppApi != null, "webAppApi null");
    this.webAppApi = webAppApi;
  }

  @Override
  public void setup() {
    Logger.getLogger("com.sun.jersey").setLevel(Level.FINEST);
    // Make one of these to ensure that the jax-b annotations
    // are properly picked up.
    bind(SliderJacksonJaxbJsonProvider.class);
    
    // Get exceptions printed to the screen
    bind(GenericExceptionHandler.class);
    // bind the REST interface
    bind(AMWebServices.class);
    //bind(AMAgentWebServices.class);
    route("/", SliderAMController.class);
    route(CONTAINER_STATS, SliderAMController.class, "containerStats");
    route(CLUSTER_SPEC, SliderAMController.class, "specification");
  }

  @Override
  public void configureServlets() {
    setup();

    serve("/", "/__stop").with(Dispatcher.class);

    for (String path : this.getServePathSpecs()) {
      serve(path).with(Dispatcher.class);
    }

    // metrics
    MetricsAndMonitoring monitoring =
        webAppApi.getMetricsAndMonitoring();
    serve(SYSTEM_HEALTHCHECK).with(new HealthCheckServlet(monitoring.getHealth()));
    serve(SYSTEM_METRICS).with(new MetricsServlet(monitoring.getMetrics()));
    serve(SYSTEM_PING).with(new PingServlet());
    serve(SYSTEM_THREADS).with(new ThreadDumpServlet());

    String regex = "(?!/ws)";
    serveRegex(regex).with(SliderDefaultWrapperServlet.class); 

    Map<String, String> params = new HashMap<>();
    params.put(ResourceConfig.FEATURE_IMPLICIT_VIEWABLES, "true");
    params.put(ServletContainer.FEATURE_FILTER_FORWARD_ON_404, "true");
    params.put(ResourceConfig.FEATURE_XMLROOTELEMENT_PROCESSING, "true");
    params.put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName());
    params.put(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName());
    //params.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
    //params.put("com.sun.jersey.spi.container.ContainerResponseFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
    //params.put("com.sun.jersey.config.feature.Trace", "true");
    params.put("com.sun.jersey.config.property.WadlGeneratorConfig",
        AMWadlGeneratorConfig.CLASSNAME);
    filter("/*").through(GuiceContainer.class, params);
  }
}
