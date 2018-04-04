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
package org.apache.hadoop.yarn.service.client;

import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ServiceStatus;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.eclipse.jetty.util.UrlEncoded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.*;

/**
 * The rest API client for users to manage services on YARN.
 */
public class ApiServiceClient extends AppAdminClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApiServiceClient.class);
  protected YarnClient yarnClient;

  @Override protected void serviceInit(Configuration configuration)
      throws Exception {
    yarnClient = YarnClient.createYarnClient();
    addService(yarnClient);
    super.serviceInit(configuration);
  }

  /**
   * Calculate Resource Manager address base on working REST API.
   */
  private String getRMWebAddress() {
    Configuration conf = getConfig();
    String scheme = "http://";
    String path = "/app/v1/services/version";
    String rmAddress = conf
        .get("yarn.resourcemanager.webapp.address");
    if (YarnConfiguration.useHttps(conf)) {
      scheme = "https://";
      rmAddress = conf
          .get("yarn.resourcemanager.webapp.https.address");
    }
    boolean useKerberos = UserGroupInformation.isSecurityEnabled();
    List<String> rmServers = RMHAUtils
        .getRMHAWebappAddresses(new YarnConfiguration(conf));
    for (String host : rmServers) {
      try {
        Client client = Client.create();
        StringBuilder sb = new StringBuilder();
        sb.append(scheme);
        sb.append(host);
        sb.append(path);
        if (!useKerberos) {
          try {
            String username = UserGroupInformation.getCurrentUser().getShortUserName();
            sb.append("?user.name=");
            sb.append(username);
          } catch (IOException e) {
            LOG.debug("Fail to resolve username: {}", e);
          }
        }
        WebResource webResource = client
            .resource(sb.toString());
        if (useKerberos) {
          AuthenticatedURL.Token token = new AuthenticatedURL.Token();
          webResource.header("WWW-Authenticate", token);
        }
        ClientResponse test = webResource.get(ClientResponse.class);
        if (test.getStatus() == 200) {
          rmAddress = host;
          break;
        }
      } catch (Exception e) {
        LOG.debug("Fail to connect to: "+host, e);
      }
    }
    return scheme+rmAddress;
  }

  /**
   * Compute active resource manager API service location.
   *
   * @param appName - YARN service name
   * @return URI to API Service
   * @throws IOException
   */
  private String getApiUrl(String appName) throws IOException {
    String url = getRMWebAddress();
    StringBuilder api = new StringBuilder();
    api.append(url);
    api.append("/app/v1/services");
    if (appName != null) {
      api.append("/");
      api.append(appName);
    }
    Configuration conf = getConfig();
    if (conf.get("hadoop.http.authentication.type").equalsIgnoreCase("simple")) {
      api.append("?user.name=" + UrlEncoded
          .encodeString(System.getProperty("user.name")));
    }
    return api.toString();
  }

  private Builder getApiClient() throws IOException {
    return getApiClient(null);
  }

  /**
   * Setup API service web request.
   *
   * @param appName
   * @return
   * @throws IOException
   */
  private Builder getApiClient(String appName) throws IOException {
    Client client = Client.create(getClientConfig());
    Configuration conf = getConfig();
    client.setChunkedEncodingSize(null);
    Builder builder = client
        .resource(getApiUrl(appName)).type(MediaType.APPLICATION_JSON);
    if (conf.get("hadoop.http.authentication.type").equals("kerberos")) {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      builder.header("WWW-Authenticate", token);
    }
    return builder
        .accept("application/json;charset=utf-8");
  }

  private ClientConfig getClientConfig() {
    ClientConfig config = new DefaultClientConfig();
    config.getProperties().put(
        ClientConfig.PROPERTY_CHUNKED_ENCODING_SIZE, 0);
    config.getProperties().put(
        ClientConfig.PROPERTY_BUFFER_RESPONSE_ENTITY_ON_EXCEPTION, true);
    return config;
  }

  private int processResponse(ClientResponse response) {
    response.bufferEntity();
    String output;
    if (response.getStatus() == 401) {
      LOG.error("Authentication required");
      return EXIT_EXCEPTION_THROWN;
    }
    if (response.getStatus() == 503) {
      LOG.error("YARN Service is unavailable or disabled.");
      return EXIT_EXCEPTION_THROWN;
    }
    try {
      ServiceStatus ss = response.getEntity(ServiceStatus.class);
      output = ss.getDiagnostics();
    } catch (Throwable t) {
      output = response.getEntity(String.class);
    }
    if (output==null) {
      output = response.getEntity(String.class);
    }
    if (response.getStatus() <= 299) {
      LOG.info(output);
      return EXIT_SUCCESS;
    } else {
      LOG.error(output);
      return EXIT_EXCEPTION_THROWN;
    }
  }

  /**
   * Utility method to load Service json from disk or from
   * YARN examples.
   *
   * @param fileName - path to yarnfile
   * @param serviceName - YARN Service Name
   * @param lifetime - application lifetime
   * @param queue - Queue to submit application
   * @return
   * @throws IOException
   * @throws YarnException
   */
  public Service loadAppJsonFromLocalFS(String fileName, String serviceName,
      Long lifetime, String queue) throws IOException, YarnException {
    File file = new File(fileName);
    if (!file.exists() && fileName.equals(file.getName())) {
      String examplesDirStr = System.getenv("YARN_SERVICE_EXAMPLES_DIR");
      String[] examplesDirs;
      if (examplesDirStr == null) {
        String yarnHome = System
            .getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
        examplesDirs = new String[]{
            yarnHome + "/share/hadoop/yarn/yarn-service-examples",
            yarnHome + "/yarn-service-examples"
        };
      } else {
        examplesDirs = StringUtils.split(examplesDirStr, ":");
      }
      for (String dir : examplesDirs) {
        file = new File(MessageFormat.format("{0}/{1}/{2}.json",
            dir, fileName, fileName));
        if (file.exists()) {
          break;
        }
        // Then look for secondary location.
        file = new File(MessageFormat.format("{0}/{1}.json",
            dir, fileName));
        if (file.exists()) {
          break;
        }
      }
    }
    if (!file.exists()) {
      throw new YarnException("File or example could not be found: " +
          fileName);
    }
    Path filePath = new Path(file.getAbsolutePath());
    LOG.info("Loading service definition from local FS: " + filePath);
    Service service = jsonSerDeser
        .load(FileSystem.getLocal(getConfig()), filePath);
    if (!StringUtils.isEmpty(serviceName)) {
      service.setName(serviceName);
    }
    if (lifetime != null && lifetime > 0) {
      service.setLifetime(lifetime);
    }
    if (!StringUtils.isEmpty(queue)) {
      service.setQueue(queue);
    }
    return service;
  }

  /**
   * Launch YARN service application.
   *
   * @param fileName - path to yarnfile
   * @param appName - YARN Service Name
   * @param lifetime - application lifetime
   * @param queue - Queue to submit application
   */
  @Override
  public int actionLaunch(String fileName, String appName, Long lifetime,
      String queue) throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      Service service =
          loadAppJsonFromLocalFS(fileName, appName, lifetime, queue);
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient()
          .post(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to launch application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  /**
   * Stop YARN service application.
   *
   * @param appName - YARN Service Name
   */
  @Override
  public int actionStop(String appName) throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      Service service = new Service();
      service.setName(appName);
      service.setState(ServiceState.STOPPED);
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient(appName)
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to stop application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  /**
   * Start YARN service application.
   *
   * @param appName - YARN Service Name
   */
  @Override
  public int actionStart(String appName) throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      Service service = new Service();
      service.setName(appName);
      service.setState(ServiceState.STARTED);
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient(appName)
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to start application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  /**
   * Save Service configuration.
   *
   * @param fileName - path to Yarnfile
   * @param appName - YARN Service Name
   * @param lifetime - container life time
   * @param queue - Queue to submit the application
   */
  @Override
  public int actionSave(String fileName, String appName, Long lifetime,
      String queue) throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      Service service =
          loadAppJsonFromLocalFS(fileName, appName, lifetime, queue);
      service.setState(ServiceState.STOPPED);
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient()
          .post(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to save application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  /**
   * Decommission a YARN service.
   *
   * @param appName - YARN Service Name
   */
  @Override
  public int actionDestroy(String appName) throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      ClientResponse response = getApiClient(appName)
          .delete(ClientResponse.class);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to destroy application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  /**
   * Change number of containers associated with a service.
   *
   * @param appName - YARN Service Name
   * @param componentCounts - list of components and desired container count
   */
  @Override
  public int actionFlex(String appName, Map<String, String> componentCounts)
      throws IOException, YarnException {
    int result = EXIT_SUCCESS;
    try {
      Service service = new Service();
      service.setName(appName);
      service.setState(ServiceState.FLEX);
      for (Map.Entry<String, String> entry : componentCounts.entrySet()) {
        Component component = new Component();
        component.setName(entry.getKey());
        Long numberOfContainers = Long.parseLong(entry.getValue());
        component.setNumberOfContainers(numberOfContainers);
        service.addComponent(component);
      }
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient(appName)
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Fail to flex application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  @Override
  public int enableFastLaunch(String destinationFolder) throws IOException, YarnException {
    ServiceClient sc = new ServiceClient();
    sc.init(getConfig());
    sc.start();
    int result = sc.enableFastLaunch(destinationFolder);
    sc.close();
    return result;
  }

  /**
   * Retrieve Service Status through REST API.
   *
   * @param appIdOrName - YARN application ID or application name
   * @return Status output
   */
  @Override
  public String getStatusString(String appIdOrName) throws IOException,
      YarnException {
    String output = "";
    String appName;
    try {
      ApplicationId appId = ApplicationId.fromString(appIdOrName);
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      appName = appReport.getName();
    } catch (IllegalArgumentException e) {
      // not app Id format, may be app name
      appName = appIdOrName;
      ServiceApiUtil.validateNameFormat(appName, getConfig());
    }
    try {
      ClientResponse response = getApiClient(appName).get(ClientResponse.class);
      if (response.getStatus() != 200) {
        StringBuilder sb = new StringBuilder();
        sb.append(appName);
        sb.append(" Failed : HTTP error code : ");
        sb.append(response.getStatus());
        return sb.toString();
      }
      output = response.getEntity(String.class);
    } catch (Exception e) {
      LOG.error("Fail to check application status: ", e);
    }
    return output;
  }

  @Override
  public int actionUpgrade(String appName,
      String fileName) throws IOException, YarnException {
    int result;
    try {
      Service service =
          loadAppJsonFromLocalFS(fileName, appName, null, null);
      service.setState(ServiceState.UPGRADING);
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient()
          .post(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to upgrade application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }
}
