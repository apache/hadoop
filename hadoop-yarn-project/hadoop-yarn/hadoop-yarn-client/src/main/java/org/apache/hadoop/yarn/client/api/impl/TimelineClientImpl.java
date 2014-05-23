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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenSelector;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

@Private
@Unstable
public class TimelineClientImpl extends TimelineClient {

  private static final Log LOG = LogFactory.getLog(TimelineClientImpl.class);
  private static final String RESOURCE_URI_STR = "/ws/v1/timeline/";
  private static final Joiner JOINER = Joiner.on("");
  private static Options opts;
  static {
    opts = new Options();
    opts.addOption("put", true, "Put the TimelineEntities in a JSON file");
    opts.getOption("put").setArgName("Path to the JSON file");
    opts.addOption("help", false, "Print usage");
  }

  private Client client;
  private URI resURI;
  private boolean isEnabled;
  private TimelineAuthenticatedURLConnectionFactory urlFactory;

  public TimelineClientImpl() {
    super(TimelineClientImpl.class.getName());
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    if (UserGroupInformation.isSecurityEnabled()) {
      urlFactory = new TimelineAuthenticatedURLConnectionFactory();
      client = new Client(new URLConnectionClientHandler(urlFactory), cc);
    } else {
      client = Client.create(cc);
    }
  }

  protected void serviceInit(Configuration conf) throws Exception {
    isEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
    if (!isEnabled) {
      LOG.info("Timeline service is not enabled");
    } else {
      if (YarnConfiguration.useHttps(conf)) {
        resURI = URI
            .create(JOINER.join("https://", conf.get(
                YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS),
                RESOURCE_URI_STR));
      } else {
        resURI = URI.create(JOINER.join("http://", conf.get(
            YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS),
            RESOURCE_URI_STR));
      }
      if (UserGroupInformation.isSecurityEnabled()) {
        urlFactory.setService(TimelineUtils.buildTimelineTokenService(conf));
      }
      LOG.info("Timeline service address: " + resURI);
    }
    super.serviceInit(conf);
  }

  @Override
  public TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException {
    if (!isEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Nothing will be put because timeline service is not enabled");
      }
      return new TimelinePutResponse();
    }
    TimelineEntities entitiesContainer = new TimelineEntities();
    entitiesContainer.addEntities(Arrays.asList(entities));
    ClientResponse resp;
    try {
      resp = doPostingEntities(entitiesContainer);
    } catch (RuntimeException re) {
      // runtime exception is expected if the client cannot connect the server
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg, re);
      throw re;
    }
    if (resp == null ||
        resp.getClientResponseStatus() != ClientResponse.Status.OK) {
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg);
      if (LOG.isDebugEnabled() && resp != null) {
        String output = resp.getEntity(String.class);
        LOG.debug("HTTP error code: " + resp.getStatus()
            + " Server response : \n" + output);
      }
      throw new YarnException(msg);
    }
    return resp.getEntity(TimelinePutResponse.class);
  }

  @Override
  public Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException, YarnException {
    return TimelineAuthenticator.getDelegationToken(resURI.toURL(),
        urlFactory.token, renewer);
  }

  @Private
  @VisibleForTesting
  public ClientResponse doPostingEntities(TimelineEntities entities) {
    WebResource webResource = client.resource(resURI);
    return webResource.accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
  }

  private static class TimelineAuthenticatedURLConnectionFactory
      implements HttpURLConnectionFactory {

    private AuthenticatedURL.Token token;
    private TimelineAuthenticator authenticator;
    private Token<TimelineDelegationTokenIdentifier> dToken;
    private Text service;

    public TimelineAuthenticatedURLConnectionFactory() {
      token = new AuthenticatedURL.Token();
      authenticator = new TimelineAuthenticator();
    }

    @Override
    public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
      try {
        if (dToken == null) {
          //TODO: need to take care of the renew case
          dToken = selectToken();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Timeline delegation token: " + dToken.toString());
          }
        }
        if (dToken != null) {
          Map<String, String> params = new HashMap<String, String>();
          TimelineAuthenticator.injectDelegationToken(params, dToken);
          url = TimelineAuthenticator.appendParams(url, params);
          if (LOG.isDebugEnabled()) {
            LOG.debug("URL with delegation token: " + url);
          }
        }
        return new AuthenticatedURL(authenticator).openConnection(url, token);
      } catch (AuthenticationException e) {
        LOG.error("Authentication failed when openning connection [" + url
            + "] with token [" + token + "].", e);
        throw new IOException(e);
      }
    }

    private Token<TimelineDelegationTokenIdentifier> selectToken() {
      UserGroupInformation ugi;
      try {
        ugi = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        String msg = "Error when getting the current user";
        LOG.error(msg, e);
        throw new YarnRuntimeException(msg, e);
      }
      TimelineDelegationTokenSelector tokenSelector =
          new TimelineDelegationTokenSelector();
      return tokenSelector.selectToken(
          service, ugi.getCredentials().getAllTokens());
    }

    public void setService(Text service) {
      this.service = service;
    }

  }

  public static void main(String[] argv) throws Exception {
    CommandLine cliParser = new GnuParser().parse(opts, argv);
    if (cliParser.hasOption("put")) {
      String path = cliParser.getOptionValue("put");
      if (path != null && path.length() > 0) {
        putTimelineEntitiesInJSONFile(path);
        return;
      }
    }
    printUsage();
  }

  /**
   * Put timeline data in a JSON file via command line.
   * 
   * @param path
   *          path to the {@link TimelineEntities} JSON file
   */
  private static void putTimelineEntitiesInJSONFile(String path) {
    File jsonFile = new File(path);
    if (!jsonFile.exists()) {
      System.out.println("Error: File [" + jsonFile.getAbsolutePath()
          + "] doesn't exist");
      return;
    }
    ObjectMapper mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
    TimelineEntities entities = null;
    try {
      entities = mapper.readValue(jsonFile, TimelineEntities.class);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace(System.err);
      return;
    }
    Configuration conf = new YarnConfiguration();
    TimelineClient client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
    try {
      if (UserGroupInformation.isSecurityEnabled()
          && conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false)) {
        Token<TimelineDelegationTokenIdentifier> token =
            client.getDelegationToken(
                UserGroupInformation.getCurrentUser().getUserName());
        UserGroupInformation.getCurrentUser().addToken(token);
      }
      TimelinePutResponse response = client.putEntities(
          entities.getEntities().toArray(
              new TimelineEntity[entities.getEntities().size()]));
      if (response.getErrors().size() == 0) {
        System.out.println("Timeline data is successfully put");
      } else {
        for (TimelinePutResponse.TimelinePutError error : response.getErrors()) {
          System.out.println("TimelineEntity [" + error.getEntityType() + ":" +
              error.getEntityId() + "] is not successfully put. Error code: " +
              error.getErrorCode());
        }
      }
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace(System.err);
    } finally {
      client.stop();
    }
  }

  /**
   * Helper function to print out usage
   */
  private static void printUsage() {
    new HelpFormatter().printHelp("TimelineClient", opts);
  }

}
