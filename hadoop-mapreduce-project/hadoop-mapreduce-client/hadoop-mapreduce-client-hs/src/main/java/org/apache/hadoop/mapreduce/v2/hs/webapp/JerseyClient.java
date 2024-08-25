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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class JerseyClient implements RestClient {

  private static final Log LOG = LogFactory.getLog(JerseyClient.class);

  private final static String FORMAT = "application/json";

  private final Client client;

  private final ObjectMapper mapper;
  public JerseyClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    clientConfig.getClasses().add(JacksonJsonProvider.class);
    client = Client.create(clientConfig);
    mapper = new ObjectMapper()
        .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true)
        .configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true)
        .configure(MapperFeature.AUTO_DETECT_CREATORS, true)
        .configure(MapperFeature.AUTO_DETECT_SETTERS, true)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(MapperFeature.USE_ANNOTATIONS, true);
  }

  public String fetchJson(String url) {
    WebResource webResource = client.resource(url);

    ClientResponse response = webResource.accept(FORMAT).get(ClientResponse.class);

    if (response.getStatus() != 200) {
      String msg = "Failed to fetch " + url + ", HTTP error code : " + response.getStatus();
      LOG.error(msg);
      if (response.getStatus() == 404) {
        // Use special handling for 404 errors
        throw new NotFoundException(msg);
      }
      throw new RuntimeException(msg);
    }
    String responseString = response.getEntity(String.class);
    if (!response.getType().toString().contains(MediaType.APPLICATION_JSON)) {
      LOG.error("Received non-json (" + response.getType() + ") response from "
          + url + ", response: " + responseString);
      return null;
    }

    return responseString;
  }

  public <T> T fetchAs(String url, Class<T> clazz){
    String json = fetchJson(url);
    if(json == null || json.trim().isEmpty()){
      LOG.info("Fetched empty response from " + url);
      return null;
    }

    try {
      return mapper.readValue(json, clazz);
    } catch (IOException e) {
      LOG.error("Error deserializing class  " + clazz.getName() + " from json " + json, e);
      return null;
    }
  }

  @Override
  public <T> T convert(String json, Class<T> clazz) {
    if(json == null || json.isEmpty()){
      return null;
    }

    try {
      return mapper.readValue(json, clazz);
    } catch (IOException e) {
      LOG.error("Error deserializing class  " + clazz.getName() + " from json " + json, e);
      return null;
    }
  }
}
