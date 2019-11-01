package org.apache.hadoop.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.core.DefaultConnectionConfig;
import com.microsoft.graph.logger.ILogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.DirectoryObject;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesPage;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequestBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.oauth2.AccessTokenProvider;
import org.apache.hadoop.security.oauth2.ClientCredsTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of GroupMappingServiceProvider that uses the
 * Microsoft Graph API (https://developer.microsoft.com/en-us/graph)
 * to retrieve user's groups.
 *
 * It does this by first requesting an Oauth2 token using the
 * credentials of the application registered in Azure.
 * This application has to have the Directory.Read.All application
 * permission for this to work.
 *
 * After acquiring the token, it queries the Graph API using this
 * token to retrieve a list of groups of the requested user.
 *
 */
public class MicrosoftGraphGroupsMapping
    implements GroupMappingServiceProvider, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(
      MicrosoftGraphGroupsMapping.class);

  public static final String MS_GRAPH_GROUPS_OAUTH2_URL_KEY =
      "hadoop.security.ms-graph.groups.oauth2.url";
  public static final String MS_GRAPH_GROUPS_OAUTH2_URL_DEFAULT = "";

  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_KEY =
      "hadoop.security.ms-graph.groups.oauth2.client-id";
  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_ID_DEFAULT = "";

  public static final String MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_KEY =
      "hadoop.security.ms-graph.groups.oauth2.client-secret.alias";
  public static final String
      MS_GRAPH_GROUPS_OAUTH2_CLIENT_SECRET_ALIAS_DEFAULT = "";

  public static final String MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_KEY =
      "hadoop.security.ms-graph.groups.oauth2.grant-type";
  public static final String MS_GRAPH_GROUPS_OAUTH2_GRANT_TYPE_DEFAULT =
      "client_credentials";

  public static final String MS_GRAPH_GROUPS_API_URL_KEY =
      "hadoop.security.ms-graph.groups.api.url";
  public static final String MS_GRAPH_GROUPS_API_URL_DEFAULT =
      "https://graph.microsoft.com";

  public static final String MS_GRAPH_GROUPS_API_ATTEMPTS_KEY =
      "hadoop.security.ms-graph.groups.api.attempts";
  public static final Integer MS_GRAPH_GROUPS_API_ATTEMPTS_DEFAULT = 2;

  public static final String MS_GRAPH_GROUPS_API_TIMEOUT_MS_KEY =
      "hadoop.security.ms-graph.groups.api.timeout";
  public static final Integer MS_GRAPH_GROUPS_API_TIMEOUT_MS_DEFAULT =
      (int) TimeUnit.SECONDS.toMillis(20);

  public static final String MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_KEY =
      "hadoop.security.ms-graph.groups.api.group.field.extract";
  public static final String MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_DEFAULT =
      "displayName";

  public static final String MS_GRAPH_GROUPS_API_USERNAME_FORMAT_KEY =
      "hadoop.security.ms-graph.groups.api.username-format";
  public static final String MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT = "%s";

  private Configuration conf;

  private IGraphServiceClient graphClient;

  private String apiGroupFieldExtract;
  private String usernameFormat;

  public MicrosoftGraphGroupsMapping() {
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
    } catch (IOException e) {
      LOG.error("Failed to get clientSecret using alias: {}",
          oauth2ClientSecretAlias);
    }

    AccessTokenProvider accessTokenProvider = new ClientCredsTokenProvider(
        oauth2Url, oauth2ClientId, oauth2ClientSecret, oauth2GrantType, apiUrl);

    // Create the client
    this.graphClient = GraphServiceClient.builder()
        .authenticationProvider(accessTokenProvider)
        .logger(new GraphLogger())
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
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override // GroupMappingServiceProvider
  public List<String> getGroups(final String user) throws IOException {

    // TODO: This is a hack to fail fast for 'hadoop' username
    // to avoid API calls and retries. Should be removed later.
    if (user.equalsIgnoreCase("hadoop")) {
      return Collections.emptyList();
    }

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

  private List<String> doGetGroups(String user) throws ClientException {
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
   * Caches groups, no need to do that for this provider
   */
  @Override // GroupMappingServiceProvider
  public void cacheGroupsRefresh() {
    // does nothing in this provider of user to groups mapping
  }

  /**
   * Adds groups to cache, no need to do that for this provider
   *
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
   * Utility class so that we can pass log4j Logger to the graph client.
   */
  private static class GraphLogger implements ILogger {

    @Override
    public void setLoggingLevel(LoggerLevel loggerLevel) {
    }

    @Override
    public LoggerLevel getLoggingLevel() {
      if (LOG.isDebugEnabled()) {
        return LoggerLevel.DEBUG;
      }
      return LoggerLevel.ERROR;
    }

    @Override
    public void logDebug(String s) {
      LOG.debug(s);
    }

    @Override
    public void logError(String s, Throwable throwable) {
      LOG.error(s, throwable);
    }
  }

  /**
   * Workaround for:
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
