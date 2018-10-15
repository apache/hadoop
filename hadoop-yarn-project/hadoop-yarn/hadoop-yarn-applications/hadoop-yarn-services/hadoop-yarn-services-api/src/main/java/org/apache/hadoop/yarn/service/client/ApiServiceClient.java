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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Base64;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ServiceStatus;
import org.apache.hadoop.yarn.service.conf.RestApiConstants;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.eclipse.jetty.util.UrlEncoded;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
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
  private static final Base64 BASE_64_CODEC = new Base64(0);
  protected YarnClient yarnClient;

  @Override protected void serviceInit(Configuration configuration)
      throws Exception {
    yarnClient = YarnClient.createYarnClient();
    addService(yarnClient);
    super.serviceInit(configuration);
  }

  /**
   * Generate SPNEGO challenge request token.
   *
   * @param server - hostname to contact
   * @throws IOException
   * @throws InterruptedException
   */
  String generateToken(String server) throws IOException, InterruptedException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    LOG.debug("The user credential is {}", currentUser);
    String challenge = currentUser
        .doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            try {
              // This Oid for Kerberos GSS-API mechanism.
              Oid mechOid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
              GSSManager manager = GSSManager.getInstance();
              // GSS name for server
              GSSName serverName = manager.createName("HTTP@" + server,
                  GSSName.NT_HOSTBASED_SERVICE);
              // Create a GSSContext for authentication with the service.
              // We're passing client credentials as null since we want them to
              // be read from the Subject.
              GSSContext gssContext = manager.createContext(
                  serverName.canonicalize(mechOid), mechOid, null,
                  GSSContext.DEFAULT_LIFETIME);
              gssContext.requestMutualAuth(true);
              gssContext.requestCredDeleg(true);
              // Establish context
              byte[] inToken = new byte[0];
              byte[] outToken = gssContext.initSecContext(inToken, 0,
                  inToken.length);
              gssContext.dispose();
              // Base64 encoded and stringified token for server
              LOG.debug("Got valid challenge for host {}", serverName);
              return new String(BASE_64_CODEC.encode(outToken),
                  StandardCharsets.US_ASCII);
            } catch (GSSException | IllegalAccessException
                | NoSuchFieldException | ClassNotFoundException e) {
              LOG.error("Error: {}", e);
              throw new AuthenticationException(e);
            }
          }
        });
    return challenge;
  }

  /**
   * Calculate Resource Manager address base on working REST API.
   */
  String getRMWebAddress() {
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
    List<String> rmServers = getRMHAWebAddresses(conf);
    for (String host : rmServers) {
      try {
        Client client = Client.create();
        client.setFollowRedirects(false);
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
        Builder builder = client
            .resource(sb.toString()).type(MediaType.APPLICATION_JSON);
        if (useKerberos) {
          String[] server = host.split(":");
          String challenge = generateToken(server[0]);
          builder.header(HttpHeaders.AUTHORIZATION, "Negotiate " +
              challenge);
          LOG.debug("Authorization: Negotiate {}", challenge);
        }
        ClientResponse test = builder.get(ClientResponse.class);
        if (test.getStatus() == 200) {
          rmAddress = host;
          break;
        }
      } catch (Exception e) {
        LOG.info("Fail to connect to: "+host);
        LOG.debug("Root cause: {}", e);
      }
    }
    return scheme+rmAddress;
  }

  List<String> getRMHAWebAddresses(Configuration conf) {
    return RMHAUtils
        .getRMHAWebappAddresses(new YarnConfiguration(conf));
  }

  /**
   * Compute active resource manager API service location.
   *
   * @param appName - YARN service name
   * @return URI to API Service
   * @throws IOException
   */
  private String getServicePath(String appName) throws IOException {
    String url = getRMWebAddress();
    StringBuilder api = new StringBuilder();
    api.append(url);
    api.append("/app/v1/services");
    if (appName != null) {
      api.append("/");
      api.append(appName);
    }
    appendUserNameIfRequired(api);
    return api.toString();
  }

  private String getInstancesPath(String appName) throws IOException {
    Preconditions.checkNotNull(appName);
    String url = getRMWebAddress();
    StringBuilder api = new StringBuilder();
    api.append(url);
    api.append("/app/v1/services/").append(appName).append("/")
        .append(RestApiConstants.COMP_INSTANCES);
    appendUserNameIfRequired(api);
    return api.toString();
  }

  private String getInstancePath(String appName, List<String> components,
      String version, List<String> containerStates) throws IOException {
    UriBuilder builder = UriBuilder.fromUri(getInstancesPath(appName));
    if (components != null && !components.isEmpty()) {
      components.forEach(compName ->
        builder.queryParam(RestApiConstants.PARAM_COMP_NAME, compName));
    }
    if (!Strings.isNullOrEmpty(version)){
      builder.queryParam(RestApiConstants.PARAM_VERSION, version);
    }
    if (containerStates != null && !containerStates.isEmpty()){
      containerStates.forEach(state ->
          builder.queryParam(RestApiConstants.PARAM_CONTAINER_STATE, state));
    }
    return builder.build().toString();
  }

  private String getComponentsPath(String appName) throws IOException {
    Preconditions.checkNotNull(appName);
    String url = getRMWebAddress();
    StringBuilder api = new StringBuilder();
    api.append(url);
    api.append("/app/v1/services/").append(appName).append("/")
        .append(RestApiConstants.COMPONENTS);
    appendUserNameIfRequired(api);
    return api.toString();
  }

  private void appendUserNameIfRequired(StringBuilder builder) {
    Configuration conf = getConfig();
    if (conf.get("hadoop.http.authentication.type").equalsIgnoreCase(
        "simple")) {
      builder.append("?user.name=").append(UrlEncoded
          .encodeString(System.getProperty("user.name")));
    }
  }

  private Builder getApiClient() throws IOException {
    return getApiClient(getServicePath(null));
  }

  /**
   * Setup API service web request.
   *
   * @param requestPath
   * @return
   * @throws IOException
   */
  private Builder getApiClient(String requestPath)
      throws IOException {
    Client client = Client.create(getClientConfig());
    Configuration conf = getConfig();
    client.setChunkedEncodingSize(null);
    Builder builder = client
        .resource(requestPath).type(MediaType.APPLICATION_JSON);
    if (conf.get("hadoop.http.authentication.type").equals("kerberos")) {
      try {
        URI url = new URI(requestPath);
        String challenge = generateToken(url.getHost());
        builder.header(HttpHeaders.AUTHORIZATION, "Negotiate " + challenge);
      } catch (Exception e) {
        throw new IOException(e);
      }
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
      ClientResponse response = getApiClient(getServicePath(appName))
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
      ClientResponse response = getApiClient(getServicePath(appName))
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
      ClientResponse response = getApiClient(getServicePath(appName))
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
      ClientResponse response = getApiClient(getServicePath(appName))
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
      ClientResponse response = getApiClient(getServicePath(appName))
          .get(ClientResponse.class);
      if (response.getStatus() == 404) {
        StringBuilder sb = new StringBuilder();
        sb.append(" Service ");
        sb.append(appName);
        sb.append(" not found");
        return sb.toString();
      }
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
  public int actionUpgradeExpress(String appName, File path)
      throws IOException, YarnException {
    int result;
    try {
      Service service =
          loadAppJsonFromLocalFS(path.getAbsolutePath(), appName, null, null);
      service.setState(ServiceState.EXPRESS_UPGRADING);
      String buffer = jsonSerDeser.toJson(service);
      LOG.info("Upgrade in progress. Please wait..");
      ClientResponse response = getApiClient(getServicePath(appName))
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to upgrade application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  @Override
  public int initiateUpgrade(String appName,
      String fileName, boolean autoFinalize) throws IOException, YarnException {
    int result;
    try {
      Service service =
          loadAppJsonFromLocalFS(fileName, appName, null, null);
      if (autoFinalize) {
        service.setState(ServiceState.UPGRADING_AUTO_FINALIZE);
      } else {
        service.setState(ServiceState.UPGRADING);
      }
      String buffer = jsonSerDeser.toJson(service);
      ClientResponse response = getApiClient(getServicePath(appName))
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to upgrade application: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  @Override
  public int actionUpgradeInstances(String appName, List<String> compInstances)
      throws IOException, YarnException {
    int result;
    Container[] toUpgrade = new Container[compInstances.size()];
    try {
      int idx = 0;
      for (String instanceName : compInstances) {
        Container container = new Container();
        container.setComponentInstanceName(instanceName);
        container.setState(ContainerState.UPGRADING);
        toUpgrade[idx++] = container;
      }
      String buffer = ServiceApiUtil.CONTAINER_JSON_SERDE.toJson(toUpgrade);
      ClientResponse response = getApiClient(getInstancesPath(appName))
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to upgrade component instance: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  @Override
  public int actionUpgradeComponents(String appName, List<String> components)
      throws IOException, YarnException {
    int result;
    Component[] toUpgrade = new Component[components.size()];
    try {
      int idx = 0;
      for (String compName : components) {
        Component component = new Component();
        component.setName(compName);
        component.setState(ComponentState.UPGRADING);
        toUpgrade[idx++] = component;
      }
      String buffer = ServiceApiUtil.COMP_JSON_SERDE.toJson(toUpgrade);
      ClientResponse response = getApiClient(getComponentsPath(appName))
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to upgrade components: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }

  @Override
  public int actionCleanUp(String appName, String userName) throws
      IOException, YarnException {
    ServiceClient sc = new ServiceClient();
    sc.init(getConfig());
    sc.start();
    int result = sc.actionCleanUp(appName, userName);
    sc.close();
    return result;
  }

  @Override
  public String getInstances(String appName, List<String> components,
      String version, List<String> containerStates) throws IOException,
      YarnException {
    try {
      String uri = getInstancePath(appName, components, version,
          containerStates);
      ClientResponse response = getApiClient(uri).get(ClientResponse.class);
      if (response.getStatus() != 200) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed: HTTP error code: ");
        sb.append(response.getStatus());
        sb.append(" ErrorMsg: ").append(response.getEntity(String.class));
        return sb.toString();
      }
      return response.getEntity(String.class);
    } catch (Exception e) {
      LOG.error("Fail to get containers {}", e);
    }
    return null;
  }

  @Override
  public int actionCancelUpgrade(
      String appName) throws IOException, YarnException {
    int result;
    try {
      Service service = new Service();
      service.setName(appName);
      service.setState(ServiceState.CANCEL_UPGRADING);
      String buffer = jsonSerDeser.toJson(service);
      LOG.info("Cancel upgrade in progress. Please wait..");
      ClientResponse response = getApiClient(getServicePath(appName))
          .put(ClientResponse.class, buffer);
      result = processResponse(response);
    } catch (Exception e) {
      LOG.error("Failed to cancel upgrade: ", e);
      result = EXIT_EXCEPTION_THROWN;
    }
    return result;
  }
}
