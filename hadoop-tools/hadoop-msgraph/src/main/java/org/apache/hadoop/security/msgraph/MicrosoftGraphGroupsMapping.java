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

package org.apache.hadoop.security.msgraph;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.core.DefaultConnectionConfig;
import com.microsoft.graph.models.extensions.DirectoryObject;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesPage;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.msgraph.oauth2.AccessTokenProvider;
import org.apache.hadoop.security.msgraph.oauth2.ClientCredsTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of GroupMappingServiceProvider that uses the
 * Microsoft Graph API (https://developer.microsoft.com/en-us/graph)
 * to retrieve user's groups.
 *
 * It does this by first requesting an OAuth2 token using the
 * credentials of the application registered in Azure.
 * This application has to have the Directory.Read.All application
 * permission for this to work.
 *
 * After acquiring the token, it queries the Graph API using this
 * token to retrieve a list of groups of the requested user.
 */
public class MicrosoftGraphGroupsMapping
    implements GroupMappingServiceProvider, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(
      MicrosoftGraphGroupsMapping.class);

  /** The URL of the Microsoft Graph OAuth2 endpoint. */
  public static final String MS_GRAPH_GROUPS_OAUTH2_URL_KEY =
      "hadoop.security.ms-graph.groups.oauth2.url";
  public static final String MS_GRAPH_GROUPS_OAUTH2_URL_DEFAULT = "";
  /** Identifier of the client to use to get the tokens. */
  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_KEY =
      "hadoop.security.ms-graph.groups.oauth2.client-id";
  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_DEFAULT = "";
  /** The alias of the secret. */
  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_KEY =
      "hadoop.security.ms-graph.groups.oauth2.client-secret.alias";
  public static final String
      MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_DEFAULT = "";
  /** Type of the credentials to use to get access. */
  public static final String MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_KEY =
      "hadoop.security.ms-graph.groups.oauth2.grant-type";
  public static final String MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_DEFAULT =
      "client_credentials";
  /** The URL of the Microsoft Graph groups endpoint. */
  public static final String MS_GRAPH_GROUPS_API_URL_KEY =
      "hadoop.security.ms-graph.groups.api.url";
  public static final String MS_GRAPH_GROUPS_API_URL_DEFAULT =
      "https://graph.microsoft.com";
  /** Number of attempts to get the groups. */
  public static final String MS_GRAPH_GROUPS_API_ATTEMPTS_KEY =
      "hadoop.security.ms-graph.groups.api.attempts";
  /** By default we try twice. */
  public static final Integer MS_GRAPH_GROUPS_API_ATTEMPTS_DEFAULT = 2;
  /** Timeout to get the groups in milliseconds. */
  public static final String MS_GRAPH_GROUPS_API_TIMEOUT_MS_KEY =
      "hadoop.security.ms-graph.groups.api.timeout";
  /** By default, we timeout after 20 seconds. */
  public static final Integer MS_GRAPH_GROUPS_API_TIMEOUT_MS_DEFAULT =
      (int) TimeUnit.SECONDS.toMillis(20);
  /** Field to extract the groups. */
  public static final String MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_KEY =
      "hadoop.security.ms-graph.groups.api.group.field.extract";
  /** By default we use the display name. */
  public static final String MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_DEFAULT =
      "displayName";
  /** Format of the user names. */
  public static final String MS_GRAPH_GROUPS_API_USERNAME_FORMAT_KEY =
      "hadoop.security.ms-graph.groups.api.username-format";
  /** By default we just use the name. */
  public static final String MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT = "%s";


  /** Configuration. */
  private Configuration conf;
  /** Client to Microsoft Graph. */
  private IGraphServiceClient graphClient;

  /** Group field to extract.*/
  private String apiGroupFieldExtract;
  /** Format of the user name. */
  private String usernameFormat;


  public MicrosoftGraphGroupsMapping() {
    // Empty constructor
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    this.conf = conf;

    String apiUrl = conf.get(MS_GRAPH_GROUPS_API_URL_KEY,
        MS_GRAPH_GROUPS_API_URL_DEFAULT);
    int apiAttempts = conf.getInt(MS_GRAPH_GROUPS_API_ATTEMPTS_KEY,
        MS_GRAPH_GROUPS_API_ATTEMPTS_DEFAULT);
    int apiTimeout = conf.getInt(MS_GRAPH_GROUPS_API_TIMEOUT_MS_KEY,
        MS_GRAPH_GROUPS_API_TIMEOUT_MS_DEFAULT);
    this.apiGroupFieldExtract = conf.get(
        MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_KEY,
        MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_DEFAULT);
    this.usernameFormat = conf.get(
        MS_GRAPH_GROUPS_API_USERNAME_FORMAT_KEY,
        MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT);

    String oauth2Url = conf.get(MS_GRAPH_GROUPS_OAUTH2_URL_KEY,
        MS_GRAPH_GROUPS_OAUTH2_URL_DEFAULT);
    String oauth2ClientId = conf.get(MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_KEY,
        MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_DEFAULT);
    String oauth2GrantType = conf.get(MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_KEY,
        MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_DEFAULT);
    String oauth2ClientSecretAlias = conf.get(
        MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_KEY,
        MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_DEFAULT);

    // Get the client secret
    String oauth2ClientSecret = "";
    try {
      char[] clientSecret = conf.getPassword(oauth2ClientSecretAlias);
      if (clientSecret != null) {
        oauth2ClientSecret = new String(clientSecret);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to get clientSecret using alias: {}",
          oauth2ClientSecretAlias, ioe);
    }

    AccessTokenProvider accessTokenProvider = new ClientCredsTokenProvider(
        oauth2Url, oauth2ClientId, oauth2ClientSecret, oauth2GrantType, apiUrl);

    // Create the client
    this.graphClient = GraphServiceClient.builder()
        .authenticationProvider(accessTokenProvider)
        .logger(new GraphLogger(LOG))
        .buildClient();

    // Set configuration
    WorkaroundConnectionConfig connectionConfig =
        new WorkaroundConnectionConfig();
    connectionConfig.setMaxRetries(apiAttempts - 1);
    connectionConfig.setConnectTimeout(apiTimeout);
    connectionConfig.setReadTimeout(apiTimeout);
    graphClient.getHttpProvider().setConnectionConfig(connectionConfig);

    LOG.info("Initialized. apiUrl: {}, apiAttempts: {}, apiTimeout: {}, " +
            "usernameFormat: {}, apiGroupFieldExtract: {}, oauth2Url: {}, " +
            "oauth2ClientId: {}, oauth2GrantType: {}",
        apiUrl, apiAttempts, apiTimeout, usernameFormat, apiGroupFieldExtract,
        oauth2Url, oauth2ClientId, oauth2GrantType);
  }

  @Override // Configurable
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Returns list of groups for a user.
   *
   * Retrieves an AzureADToken first and then queries the
   * Microsoft Graph API to get the groups.
   *
   * @param user Get groups for this user
   * @return List of groups for a given user
   */
  @Override // GroupMappingServiceProvider
  public List<String> getGroups(final String user) throws IOException {
    try {
      List<String> groups = doGetGroups(user);
      LOG.debug("doGetGroups({}) returned {}", user, groups);
      return groups;
    } catch (Exception e) {
      LOG.warn("Exception in getting groups for {}, exception message: {}.",
          user, e);
    }
    return Collections.emptyList();
  }

  /**
   * Get the groups for a user.
   * @param user User to query.
   * @return List of groups for this user.
   */
  private List<String> doGetGroups(final String user) throws ClientException {
    final List<String> groups = new ArrayList<>();

    String username = String.format(usernameFormat, user);

    IDirectoryObjectCollectionWithReferencesPage page =
        graphClient.users(username).memberOf().buildRequest().get();

    while (page != null) {
      List<String> groupsInPage = getGroupsFromPage(page);
      groups.addAll(groupsInPage);

      // Go to the next page
      IDirectoryObjectCollectionWithReferencesRequestBuilder builder =
          page.getNextPage();

      if (builder == null) {
        page = null;
      } else {
        page = builder.buildRequest().get();
      }
    }

    return groups;
  }

  /**
   * Get the groups from a page.
   * @param page Page to check.
   * @return List of groups in this page.
   */
  private List<String> getGroupsFromPage(
      IDirectoryObjectCollectionWithReferencesPage page) {
    List<String> groups = new ArrayList<>();

    for (DirectoryObject directoryObject : page.getCurrentPage()) {
      JsonObject groupObject = directoryObject.getRawObject();

      JsonElement groupElement = groupObject.get(apiGroupFieldExtract);
      if (groupElement != null && groupElement.isJsonPrimitive()) {
        groups.add(groupElement.getAsString());
      }
    }

    return groups;
  }

  /**
   * Caches groups, no need to do that for this provider.
   */
  @Override // GroupMappingServiceProvider
  public void cacheGroupsRefresh() {
    // does nothing in this provider of user to groups mapping
  }

  /**
   * Adds groups to cache, no need to do that for this provider.
   * @param groups unused
   */
  @Override // GroupMappingServiceProvider
  public void cacheGroupsAdd(List<String> groups) {
    // does nothing in this provider of user to groups mapping
  }

  @VisibleForTesting
  IGraphServiceClient getGraphClient() {
    return this.graphClient;
  }

  @VisibleForTesting
  void setGraphClient(IGraphServiceClient graphClient) {
    this.graphClient = graphClient;
  }

  /**
   * Workaround for
   * https://github.com/microsoftgraph/msgraph-sdk-java/issues/317
   */
  private static class WorkaroundConnectionConfig
      extends DefaultConnectionConfig {

    private int maxRetries = super.getMaxRetries();

    @Override
    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    @Override
    public int getMaxRetries() {
      return maxRetries;
    }
  }
}