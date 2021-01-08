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
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;

@Private
@Evolving
public class TimelineClientImpl extends TimelineClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineClientImpl.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String RESOURCE_URI_STR_V1 = "/ws/v1/timeline/";

  private static Options opts;
  private static final String ENTITY_DATA_TYPE = "entity";
  private static final String DOMAIN_DATA_TYPE = "domain";

  static {
    opts = new Options();
    opts.addOption("put", true, "Put the timeline entities/domain in a JSON file");
    opts.getOption("put").setArgName("Path to the JSON file");
    opts.addOption(ENTITY_DATA_TYPE, false, "Specify the JSON file contains the entities");
    opts.addOption(DOMAIN_DATA_TYPE, false, "Specify the JSON file contains the domain");
    opts.addOption("help", false, "Print usage");
  }

  @VisibleForTesting
  protected DelegationTokenAuthenticatedURL.Token token;
  @VisibleForTesting
  protected UserGroupInformation authUgi;
  @VisibleForTesting
  protected String doAsUser;

  private boolean timelineServiceV15Enabled;
  private TimelineWriter timelineWriter;

  private String timelineServiceAddress;

  @Private
  @VisibleForTesting
  TimelineConnector connector;

  public TimelineClientImpl() {
    super(TimelineClientImpl.class.getName());
  }

  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceV1Enabled(conf)) {
      throw new IOException("Timeline V1 client is not properly configured. "
          + "Either timeline service is not enabled or version is not set to"
          + " 1.x");
    }

    timelineServiceV15Enabled =
        YarnConfiguration.timelineServiceV15Enabled(conf);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUgi = ugi.getRealUser();
    if (realUgi != null) {
      authUgi = realUgi;
      doAsUser = ugi.getShortUserName();
    } else {
      authUgi = ugi;
      doAsUser = null;
    }
    token = new DelegationTokenAuthenticatedURL.Token();
    connector = createTimelineConnector();

    if (YarnConfiguration.useHttps(conf)) {
      timelineServiceAddress =
          conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      timelineServiceAddress =
          conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
    LOG.info("Timeline service address: " + getTimelineServiceAddress());
    super.serviceInit(conf);
  }

  @VisibleForTesting
  protected TimelineConnector createTimelineConnector() {
    TimelineConnector newConnector =
        new TimelineConnector(true, authUgi, doAsUser, token);
    addIfService(newConnector);
    return newConnector;
  }

  @Override
  protected void serviceStart() throws Exception {
    timelineWriter = createTimelineWriter(getConfig(), authUgi,
        connector.getClient(), TimelineConnector.constructResURI(getConfig(),
            timelineServiceAddress, RESOURCE_URI_STR_V1));
  }

  protected TimelineWriter createTimelineWriter(Configuration conf,
      UserGroupInformation ugi, Client webClient, URI uri)
      throws IOException {
    if (timelineServiceV15Enabled) {
      return new FileSystemTimelineWriter(
          conf, ugi, webClient, uri);
    } else {
      return new DirectTimelineWriter(ugi, webClient, uri);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.timelineWriter != null) {
      this.timelineWriter.close();
    }
    super.serviceStop();
  }

  @Override
  public void flush() throws IOException {
    if (timelineWriter != null) {
      timelineWriter.flush();
    }
  }

  @Override
  public TimelinePutResponse putEntities(TimelineEntity... entities)
      throws IOException, YarnException {
    return timelineWriter.putEntities(entities);
  }

  @Override
  public void putDomain(TimelineDomain domain) throws IOException,
      YarnException {
    timelineWriter.putDomain(domain);
  }

  private String getTimelineServiceAddress() {
    return this.timelineServiceAddress;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException, YarnException {
    PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>
        getDTAction =
        new PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>() {

          @Override
          public Token<TimelineDelegationTokenIdentifier> run()
              throws Exception {
            DelegationTokenAuthenticatedURL authUrl =
                connector.getDelegationTokenAuthenticatedURL();
            // TODO we should add retry logic here if timelineServiceAddress is
            // not available immediately.
            return (Token) authUrl.getDelegationToken(
                TimelineConnector.constructResURI(getConfig(),
                    getTimelineServiceAddress(), RESOURCE_URI_STR_V1).toURL(),
                token, renewer, doAsUser);
          }
        };
    return (Token<TimelineDelegationTokenIdentifier>) connector
        .operateDelegationToken(getDTAction);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long renewDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
          throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Long> renewDTAction =
        new PrivilegedExceptionAction<Long>() {

          @Override
          public Long run() throws Exception {
            // If the timeline DT to renew is different than cached, replace it.
            // Token to set every time for retry, because when exception
            // happens, DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                connector.getDelegationTokenAuthenticatedURL();
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty
                ? TimelineConnector.constructResURI(getConfig(),
                    getTimelineServiceAddress(), RESOURCE_URI_STR_V1)
                : new URI(scheme, null, address.getHostName(),
                    address.getPort(), RESOURCE_URI_STR_V1, null, null);
            return authUrl
                .renewDelegationToken(serviceURI.toURL(), token, doAsUser);
          }
        };
    return (Long) connector.operateDelegationToken(renewDTAction);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void cancelDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
      throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Void> cancelDTAction =
        new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception {
            // If the timeline DT to cancel is different than cached, replace
            // it.
            // Token to set every time for retry, because when exception
            // happens, DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                connector.getDelegationTokenAuthenticatedURL();
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty
                ? TimelineConnector.constructResURI(getConfig(),
                    getTimelineServiceAddress(), RESOURCE_URI_STR_V1)
                : new URI(scheme, null, address.getHostName(),
                    address.getPort(), RESOURCE_URI_STR_V1, null, null);
            authUrl.cancelDelegationToken(serviceURI.toURL(), token, doAsUser);
            return null;
          }
        };
    connector.operateDelegationToken(cancelDTAction);
  }

  @Override
  public String toString() {
    return super.toString() + " with timeline server "
        + TimelineConnector.constructResURI(getConfig(),
            getTimelineServiceAddress(), RESOURCE_URI_STR_V1)
        + " and writer " + timelineWriter;
  }

  public static void main(String[] argv) throws Exception {
    CommandLine cliParser = new GnuParser().parse(opts, argv);
    if (cliParser.hasOption("put")) {
      String path = cliParser.getOptionValue("put");
      if (path != null && path.length() > 0) {
        if (cliParser.hasOption(ENTITY_DATA_TYPE)) {
          putTimelineDataInJSONFile(path, ENTITY_DATA_TYPE);
          return;
        } else if (cliParser.hasOption(DOMAIN_DATA_TYPE)) {
          putTimelineDataInJSONFile(path, DOMAIN_DATA_TYPE);
          return;
        }
      }
    }
    printUsage();
  }

  /**
   * Put timeline data in a JSON file via command line.
   * 
   * @param path
   *          path to the timeline data JSON file
   * @param type
   *          the type of the timeline data in the JSON file
   */
  private static void putTimelineDataInJSONFile(String path, String type) {
    File jsonFile = new File(path);
    if (!jsonFile.exists()) {
      LOG.error("File [" + jsonFile.getAbsolutePath() + "] doesn't exist");
      return;
    }
    YarnJacksonJaxbJsonProvider.configObjectMapper(MAPPER);
    TimelineEntities entities = null;
    TimelineDomains domains = null;
    try {
      if (type.equals(ENTITY_DATA_TYPE)) {
        entities = MAPPER.readValue(jsonFile, TimelineEntities.class);
      } else if (type.equals(DOMAIN_DATA_TYPE)){
        domains = MAPPER.readValue(jsonFile, TimelineDomains.class);
      }
    } catch (Exception e) {
      LOG.error("Error when reading  " + e.getMessage());
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
      if (type.equals(ENTITY_DATA_TYPE)) {
        TimelinePutResponse response = client.putEntities(
            entities.getEntities().toArray(
                new TimelineEntity[entities.getEntities().size()]));
        if (response.getErrors().size() == 0) {
          LOG.info("Timeline entities are successfully put");
        } else {
          for (TimelinePutResponse.TimelinePutError error : response.getErrors()) {
            LOG.error("TimelineEntity [" + error.getEntityType() + ":" +
                error.getEntityId() + "] is not successfully put. Error code: " +
                error.getErrorCode());
          }
        }
      } else if (type.equals(DOMAIN_DATA_TYPE) && domains != null) {
        boolean hasError = false;
        for (TimelineDomain domain : domains.getDomains()) {
          try {
            client.putDomain(domain);
          } catch (Exception e) {
            LOG.error("Error when putting domain " + domain.getId(), e);
            hasError = true;
          }
        }
        if (!hasError) {
          LOG.info("Timeline domains are successfully put");
        }
      }
    } catch(RuntimeException e) {
      LOG.error("Error when putting the timeline data", e);
    } catch (Exception e) {
      LOG.error("Error when putting the timeline data", e);
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

  @VisibleForTesting
  @Private
  public UserGroupInformation getUgi() {
    return authUgi;
  }

  @Override
  public TimelinePutResponse putEntities(ApplicationAttemptId appAttemptId,
      TimelineEntityGroupId groupId, TimelineEntity... entities)
      throws IOException, YarnException {
    if (!timelineServiceV15Enabled) {
      throw new YarnException(
        "This API is not supported under current Timeline Service Version:");
    }

    return timelineWriter.putEntities(appAttemptId, groupId, entities);
  }

  @Override
  public void putDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException, YarnException {
    if (!timelineServiceV15Enabled) {
      throw new YarnException(
        "This API is not supported under current Timeline Service Version:");
    }
    timelineWriter.putDomain(appAttemptId, domain);
  }

  @Private
  @VisibleForTesting
  public void setTimelineWriter(TimelineWriter writer) {
    this.timelineWriter = writer;
  }
}
