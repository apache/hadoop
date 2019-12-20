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

package org.apache.hadoop.yarn.appcatalog.application;

import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.client.ApiServiceClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * Driver class for calling YARN Resource Manager REST API.
 */
public class YarnServiceClient {

  private static final Log LOG = LogFactory.getLog(YarnServiceClient.class);
  private static Configuration conf = new Configuration();
  private static ClientConfig getClientConfig() {
    ClientConfig config = new DefaultClientConfig();
    config.getProperties().put(ClientConfig.PROPERTY_CHUNKED_ENCODING_SIZE, 0);
    config.getProperties()
        .put(ClientConfig.PROPERTY_BUFFER_RESPONSE_ENTITY_ON_EXCEPTION, true);
    return config;
  }

  private ApiServiceClient asc;

  public YarnServiceClient() {
    try {
      asc = new ApiServiceClient(conf);
    } catch (Exception e) {
      LOG.error("Error initialize YARN Service Client: {}", e);
    }
  }

  public void createApp(Service app) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    ClientResponse response;
    try {
      boolean useKerberos = UserGroupInformation.isSecurityEnabled();
      if (useKerberos) {
        KerberosPrincipal kerberos = new KerberosPrincipal();
        String[] temp = System.getenv("PRINCIPAL").split("@");
        String[] temp2 = temp[0].split("/");
        StringBuilder sb = new StringBuilder();
        sb.append(temp2[0]);
        sb.append("/");
        sb.append("_HOST");
        sb.append("@");
        sb.append(temp[1]);
        String keytab = System.getenv("KEYTAB");
        if (!keytab.startsWith("file://")) {
          keytab = "file://" + keytab;
        }
        kerberos.setPrincipalName(sb.toString());
        kerberos.setKeytab(keytab);
        app.setKerberosPrincipal(kerberos);
      }
      response = asc.getApiClient().post(ClientResponse.class,
          mapper.writeValueAsString(app));
      if (response.getStatus() >= 299) {
        String message = response.getEntity(String.class);
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatus() + " error: " + message);
      }
    } catch (UniformInterfaceException | ClientHandlerException
        | IOException e) {
      LOG.error("Error in deploying application: ", e);
    }
  }

  public void deleteApp(String appInstanceId) {
    ClientResponse response;
    try {
      response = asc.getApiClient(asc.getServicePath(appInstanceId))
          .delete(ClientResponse.class);
      if (response.getStatus() >= 299) {
        String message = response.getEntity(String.class);
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatus() + " error: " + message);
      }
    } catch (UniformInterfaceException | ClientHandlerException
        | IOException e) {
      LOG.error("Error in deleting application: ", e);
    }
  }

  public void restartApp(Service app) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    String appInstanceId = app.getName();
    String yarnFile = mapper.writeValueAsString(app);
    ClientResponse response;
    try {
      response = asc.getApiClient(asc.getServicePath(appInstanceId))
          .put(ClientResponse.class, yarnFile);
      if (response.getStatus() >= 299) {
        String message = response.getEntity(String.class);
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatus() + " error: " + message);
      }
    } catch (UniformInterfaceException | ClientHandlerException
        | IOException e) {
      LOG.error("Error in restarting application: ", e);
    }
  }

  public void stopApp(Service app) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    String appInstanceId = app.getName();
    String yarnFile = mapper.writeValueAsString(app);
    ClientResponse response;
    try {
      response = asc.getApiClient(asc.getServicePath(appInstanceId))
          .put(ClientResponse.class, yarnFile);
      if (response.getStatus() >= 299) {
        String message = response.getEntity(String.class);
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatus() + " error: " + message);
      }
    } catch (UniformInterfaceException | ClientHandlerException
        | IOException e) {
      LOG.error("Error in stopping application: ", e);
    }
  }

  public void getStatus(AppEntry entry) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    String appInstanceId = entry.getName();
    Service app = null;
    try {
      String yarnFile = asc.getApiClient(asc.getServicePath(appInstanceId))
          .get(String.class);
      app = mapper.readValue(yarnFile, Service.class);
      entry.setYarnfile(app);
    } catch (UniformInterfaceException | IOException e) {
      LOG.error("Error in fetching application status: ", e);
    }
  }

  public void upgradeApp(Service app) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    String appInstanceId = app.getName();
    app.setState(ServiceState.EXPRESS_UPGRADING);
    String yarnFile = mapper.writeValueAsString(app);
    ClientResponse response;
    try {
      response = asc.getApiClient(asc.getServicePath(appInstanceId))
          .put(ClientResponse.class, yarnFile);
      if (response.getStatus() >= 299) {
        String message = response.getEntity(String.class);
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatus() + " error: " + message);
      }
    } catch (UniformInterfaceException | ClientHandlerException
        | IOException e) {
      LOG.error("Error in stopping application: ", e);
    }
  }
}
