/*
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

package org.apache.slider.server.appmaster.web.rest.publisher;

import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.core.registry.docstore.UriMap;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static  org.apache.slider.server.appmaster.web.rest.RestPaths.*;

/**
 * This publishes configuration sets
 */
public class PublisherResource extends AbstractSliderResource {
  protected static final Logger log =
      LoggerFactory.getLogger(PublisherResource.class);
  public static final String EXPORTS_NAME = "exports";
  public static final String EXPORTS_RESOURCES_PATH = "/" + EXPORTS_NAME;
  public static final String EXPORT_RESOURCE_PATH = EXPORTS_RESOURCES_PATH + "/{exportname}" ;
  public static final String SET_NAME =
      "{setname: " + PUBLISHED_CONFIGURATION_SET_REGEXP + "}";
  public static final String SETNAME = "setname";
  public static final String CLASSPATH = "/classpath";
  public static final String CONFIG = "config";
  
  public static final String SETNAME_PATTERN = 
      "{"+ SETNAME+": " + PUBLISHED_CONFIGURATION_SET_REGEXP + "}";
  private static final String CONFIG_PATTERN =
      SETNAME_PATTERN + "/{"+ CONFIG +": " + PUBLISHED_CONFIGURATION_REGEXP + "}";
  private final StateAccessForProviders appState;

  public PublisherResource(WebAppApi slider) {
    super(slider);
    appState = slider.getAppState();
  }

  private void init(HttpServletResponse res, UriInfo uriInfo) {
    res.setContentType(null);
    log.debug(uriInfo.getRequestUri().toString());
  }
 
  /**
   * Get a named config set 
   * @param setname name of the config set
   * @return the config set
   * @throws NotFoundException if there was no matching set
   */
  private PublishedConfigSet getConfigSet(String setname) {
    PublishedConfigSet configSet =
        appState.getPublishedConfigSet(setname);
    if (configSet == null) {
      throw new NotFoundException("Not found: " + setname);
    }
    return configSet;
  }

  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_JSON})
  public UriMap enumConfigSets(
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    init(res, uriInfo);
    String baseURL = uriInfo.getRequestUri().toString();
    if (!baseURL.endsWith("/")) {
      baseURL += "/";
    }
    UriMap uriMap = new UriMap();
    for (String name : appState.listConfigSets()) {
      uriMap.put(name, baseURL + name);
      log.info("registering config set {} at {}", name, baseURL);
    }
    uriMap.put(EXPORTS_NAME, baseURL + EXPORTS_NAME);
    return uriMap;
  }

  @GET
  @Path(CLASSPATH)
  @Produces({MediaType.APPLICATION_JSON})
  public Set<URL> getAMClassPath() {
    URL[] urls = ((URLClassLoader) getClass().getClassLoader()).getURLs();
    return new LinkedHashSet<URL>(Arrays.asList(urls));
  }

  @GET
  @Path(EXPORTS_RESOURCES_PATH)
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedExportsSet gePublishedExports() {

    return appState.getPublishedExportsSet();
  }

  @GET
  @Path(EXPORT_RESOURCE_PATH)
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedExports getAMExports2(@PathParam("exportname") String exportname,
                              @Context UriInfo uriInfo,
                              @Context HttpServletResponse res) {
    init(res, uriInfo);
    PublishedExportsSet set = appState.getPublishedExportsSet();
    return set.get(exportname);
  }

  @GET
  @Path("/"+ SETNAME_PATTERN)
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedConfigSet getPublishedConfiguration(
      @PathParam(SETNAME) String setname,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    init(res, uriInfo);

    logRequest(uriInfo);
    PublishedConfigSet publishedConfigSet = getConfigSet(setname);
    log.debug("Number of configurations: {}", publishedConfigSet.size());
    return publishedConfigSet.shallowCopy();
  }

  private void logRequest(UriInfo uriInfo) {
    log.info(uriInfo.getRequestUri().toString());
  }

  @GET
  @Path("/" + CONFIG_PATTERN)
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedConfiguration getConfigurationInstance(
      @PathParam(SETNAME) String setname,
      @PathParam(CONFIG) String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    init(res, uriInfo);

    PublishedConfiguration publishedConfig =
        getPublishedConfiguration(setname, config);
    if (publishedConfig == null) {
      log.info("Configuration {} not found", config);
      throw new NotFoundException("Not found: " + uriInfo.getAbsolutePath());
    }
    return publishedConfig;
  }

  /**
   * Get a configuration
   * @param setname name of the config set
   * @param config config
   * @return null if there was a config, but not a set
   * @throws NotFoundException if there was no matching set
   */
  public PublishedConfiguration getPublishedConfiguration(String setname,
      String config) {
    return getConfigSet(setname).get(config);
  }

  @GET
  @Path("/" + CONFIG_PATTERN + ".json")
  @Produces({MediaType.APPLICATION_JSON})
  public String getConfigurationContentJson(
      @PathParam(SETNAME) String setname,

      @PathParam(CONFIG) String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {
    return getStringRepresentation(setname, config, uriInfo, res,
        ConfigFormat.JSON);
  }

  @GET
  @Path("/" + CONFIG_PATTERN + ".xml")
  @Produces({MediaType.APPLICATION_XML})
  public String getConfigurationContentXML(
      @PathParam(SETNAME) String setname,
      @PathParam(CONFIG) String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {
    return getStringRepresentation(setname, config, uriInfo, res,
        ConfigFormat.XML);
  }
  
  @GET
  @Path("/" + CONFIG_PATTERN + ".properties")
  @Produces({MediaType.APPLICATION_XML})
  public String getConfigurationContentProperties(
      @PathParam(SETNAME) String setname,

      @PathParam(CONFIG) String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {

    return getStringRepresentation(setname, config, uriInfo, res,
        ConfigFormat.PROPERTIES);
  }

  public String getStringRepresentation(String setname,
      String config,
      UriInfo uriInfo,
      HttpServletResponse res, ConfigFormat format) throws IOException {
    // delegate (including init)
    PublishedConfiguration publishedConfig =
        getConfigurationInstance(setname, config, uriInfo, res);
    PublishedConfigurationOutputter outputter =
        publishedConfig.createOutputter(format);
    return outputter.asString();
  }

  @GET
  @Path("/" + CONFIG_PATTERN +"/{propertyName}")
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String,String> getConfigurationProperty(
      @PathParam(SETNAME) String setname,
      @PathParam(CONFIG) String config,
      @PathParam("propertyName") String propertyName,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    PublishedConfiguration publishedConfig =
        getConfigurationInstance(setname, config, uriInfo, res);
    String propVal = publishedConfig.entries.get(propertyName);
    if (propVal == null) {
      log.debug("Configuration property {} not found in configuration {}",
          propertyName, config);
      throw new NotFoundException("Property not found: " + propertyName);
    }
    Map<String, String> rtnVal = new HashMap<>();
    rtnVal.put(propertyName, propVal);

    return rtnVal;
  }
  
}
