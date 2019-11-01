package org.apache.hadoop.security;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.core.IConnectionConfig;
import com.microsoft.graph.models.extensions.DirectoryObject;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesPage;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequest;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequestBuilder;
import com.microsoft.graph.requests.extensions.IUserRequestBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.security.MicrosoftGraphGroupsMapping.MS_GRAPH_GROUPS_API_ATTEMPTS_KEY;
import static org.apache.hadoop.security.MicrosoftGraphGroupsMapping.MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_KEY;
import static org.apache.hadoop.security.MicrosoftGraphGroupsMapping.MS_GRAPH_GROUPS_API_TIMEOUT_MS_KEY;
import static org.apache.hadoop.security.MicrosoftGraphGroupsMapping.MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT;
import static org.apache.hadoop.security.MicrosoftGraphGroupsMapping.MS_GRAPH_GROUPS_API_USERNAME_FORMAT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestMicrosoftGraphGroupsMapping {

  private Configuration conf;

  @Before
  public void setUp() {
    this.conf = new Configuration();
  }

  @Test
  public void testApiConfiguraton() {
    int attempts = 7;
    int timeout = 12345;

    conf.setInt(MS_GRAPH_GROUPS_API_ATTEMPTS_KEY, attempts);
    conf.setInt(MS_GRAPH_GROUPS_API_TIMEOUT_MS_KEY, timeout);

    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = groupsMapping.getGraphClient();
    IConnectionConfig config = graphClient.getHttpProvider()
        .getConnectionConfig();

    assertEquals("Retries should be 1 less than the configured attempts",
        attempts - 1, config.getMaxRetries());
    assertEquals("Connection timeout should be configured",
        timeout, config.getConnectTimeout());
    assertEquals("Read timeout should be configured",
        timeout, config.getReadTimeout());
  }

  @Test
  public void testUsesUsernameFormat() throws IOException {
    // 1. Test the default
    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = mock(IGraphServiceClient.class);
    groupsMapping.setGraphClient(graphClient);

    groupsMapping.getGroups("bob");
    groupsMapping.getGroups("alice");

    verify(graphClient, atLeastOnce()).users(
        eq(String.format(MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT, "bob")));
    verify(graphClient, atLeastOnce()).users(
        eq(String.format(MS_GRAPH_GROUPS_API_USERNAME_FORMAT_DEFAULT, "alice")));

    // 2. Change the format
    conf.set(MS_GRAPH_GROUPS_API_USERNAME_FORMAT_KEY, "format_%s");
    groupsMapping = getGroupsMapping();
    groupsMapping.setGraphClient(graphClient);

    groupsMapping.getGroups("bob");
    groupsMapping.getGroups("alice");

    verify(graphClient, atLeastOnce()).users(eq("format_bob"));
    verify(graphClient, atLeastOnce()).users(eq("format_alice"));
  }

  /**
   * Test what happens if the Graph Client fails.
   */
  @Test
  public void testApiCallFails() throws IOException {
    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = mock(IGraphServiceClient.class);
    when(graphClient.users(anyString())).thenThrow(
        new ClientException("exception", new IOException()));

    groupsMapping.setGraphClient(graphClient);

    List<String> groups = groupsMapping.getGroups("bob");
    assertTrue("Groups should be empty when the API throws an exception",
        groups.isEmpty());
    verify(graphClient, atLeastOnce()).users(anyString());

    reset(graphClient);
    when(graphClient.users(anyString())).thenThrow(new RuntimeException());
    groups = groupsMapping.getGroups("alice");
    assertTrue("Groups should be empty when the API throws an exception",
        groups.isEmpty());

    verify(graphClient, atLeastOnce()).users(anyString());
  }

  /**
   * Test that a successful response from the Graph API is correctly parsed.
   */
  @Test
  public void testParseApiResponse() throws Exception {
    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = mock(IGraphServiceClient.class);
    groupsMapping.setGraphClient(graphClient);

    IDirectoryObjectCollectionWithReferencesPage bobResult
        = mockDirectoryPage("/msgraph_api_response1.txt");
    mockResultForUser("bob", graphClient, bobResult);

    IDirectoryObjectCollectionWithReferencesPage aliceResult
        = mockDirectoryPage("/msgraph_api_response2.txt");
    mockResultForUser("alice", graphClient, aliceResult);

    // Check the expected groups are returned
    checkGroups(groupsMapping.getGroups("bob"), "group 1", "Group_2", "group3");
    checkGroups(groupsMapping.getGroups("alice"), "Graph Group 3", "GROUP-4");

    verify(graphClient, atLeastOnce()).users(eq("alice"));
    verify(graphClient, atLeastOnce()).users(eq("bob"));
  }

  /**
   * Test that a successful response from the Graph API is correctly parsed
   * when a different field representing the group name is configured.
   */
  @Test
  public void testParseApiResponseDifferentField() throws Exception {
    conf.set(MS_GRAPH_GROUPS_API_GROUP_FIELD_EXTRACT_KEY, "id");
    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = mock(IGraphServiceClient.class);
    groupsMapping.setGraphClient(graphClient);

    IDirectoryObjectCollectionWithReferencesPage bobResult
        = mockDirectoryPage("/msgraph_api_response1.txt");
    mockResultForUser("admin", graphClient, bobResult);

    // Check the expected groups are returned
    checkGroups(groupsMapping.getGroups("admin"), "id1", "id2", "id3");
    verify(graphClient, atLeastOnce()).users(eq("admin"));
  }

  /**
   * MSGraph has paging (maximum number of groups per response) and in such
   * case the client (us) might have to retrieve the next page to get
   * all the groups.
   * https://docs.microsoft.com/en-us/graph/paging
   */
  @Test
  public void testApiResponseMultiplePage() throws Exception {
    MicrosoftGraphGroupsMapping groupsMapping = getGroupsMapping();

    IGraphServiceClient graphClient = mock(IGraphServiceClient.class);
    groupsMapping.setGraphClient(graphClient);

    IDirectoryObjectCollectionWithReferencesPage page1
        = mockDirectoryPage("/msgraph_api_response1.txt");
    IDirectoryObjectCollectionWithReferencesPage page2
        = mockDirectoryPage("/msgraph_api_response2.txt");

    // Mock getting the next page
    addNextPage(page1, page2);

    mockResultForUser("user", graphClient, page1);

    // Check that groups from both pages are returned
    checkGroups(groupsMapping.getGroups("user"), "group 1", "Group_2", "group3",
        "Graph Group 3", "GROUP-4");

    verify(graphClient, atLeastOnce()).users(eq("user"));
  }

  /**
   * Helper function for linking directory pages.
   * @param first the first directory page
   * @param next the page to come after first
   */
  private void addNextPage(
      IDirectoryObjectCollectionWithReferencesPage first,
      IDirectoryObjectCollectionWithReferencesPage next) {
    IDirectoryObjectCollectionWithReferencesRequest request =
        mock(IDirectoryObjectCollectionWithReferencesRequest.class);
    when(request.get()).thenReturn(next);

    IDirectoryObjectCollectionWithReferencesRequestBuilder builder =
        mock(IDirectoryObjectCollectionWithReferencesRequestBuilder.class);
    when(builder.buildRequest()).thenReturn(request);

    when(first.getNextPage()).thenReturn(builder);
  }

  private void mockResultForUser(
      String username, IGraphServiceClient graphClient,
      IDirectoryObjectCollectionWithReferencesPage page) {

    IDirectoryObjectCollectionWithReferencesRequest mockRequest =
        mock(IDirectoryObjectCollectionWithReferencesRequest.class);
    when(mockRequest.get()).thenReturn(page);

    IDirectoryObjectCollectionWithReferencesRequestBuilder
        referencesRequestBuilder =
        mock(IDirectoryObjectCollectionWithReferencesRequestBuilder.class);
    when(referencesRequestBuilder.buildRequest()).thenReturn(mockRequest);

    IUserRequestBuilder mockRequestBuilder = mock(IUserRequestBuilder.class);
    when(mockRequestBuilder.memberOf()).thenReturn(referencesRequestBuilder);

    when(graphClient.users(eq(username))).thenReturn(mockRequestBuilder);
  }

  private IDirectoryObjectCollectionWithReferencesPage mockDirectoryPage(
      String pageJsonFile) throws Exception {
    Gson gson = new Gson();
    URL url = getClass().getResource(pageJsonFile);
    JsonArray object = gson.fromJson(new FileReader(new File(url.getPath())),
        JsonArray.class);

    List<DirectoryObject> directoryObjects = new ArrayList<>();
    for (JsonElement jsonElement : object) {
      DirectoryObject directoryObject = mock(DirectoryObject.class);
      when(directoryObject.getRawObject()).thenReturn(
          jsonElement.getAsJsonObject());
      directoryObjects.add(directoryObject);
    }

    IDirectoryObjectCollectionWithReferencesPage page =
        mock(IDirectoryObjectCollectionWithReferencesPage.class);
    when(page.getCurrentPage()).thenReturn(directoryObjects);

    return page;
  }

  private static void checkGroups(List<String> actual, String... expected) {
    assertEquals(actual.size(), expected.length);

    for (String expectedGroup : expected) {
      assertTrue("Expected: " + expectedGroup + ", actual: " + actual,
          actual.contains(expectedGroup));
    }
  }

  private MicrosoftGraphGroupsMapping getGroupsMapping() {
    MicrosoftGraphGroupsMapping groupsMapping =
        new MicrosoftGraphGroupsMapping();
    groupsMapping.setConf(conf);
    return groupsMapping;
  }

  @Ignore
  public static void main(String[] args) {
    MicrosoftGraphGroupsMapping groupsMapping =
        new MicrosoftGraphGroupsMapping();
    groupsMapping.setConf(new Configuration());

    String user = args[0];

    long start;
    try {
      start = Time.monotonicNow();
      List<String> groups = groupsMapping.getGroups(user);
      System.out.println(user + " : " + StringUtils.join(",", groups) +
          " took: " + (Time.monotonicNow() - start) + "ms");
    } catch (IOException e) {
      System.err.println("Caught exception: " + e);
    }
  }
}
