package org.apache.hadoop.fs.azurebfs;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockUserBoundSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_END_USER_OBJECT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTests extends AbstractAbfsIntegrationTest {

  private static Path testPath = new Path("/test.txt");
  private static final String TEST_OBJECT_ID = "123456789";

  protected IntegrationTests() throws Exception {
    //todo: add later
//    String sasProvider = getRawConfiguration().get(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
//    assumeThat(MockUserBoundSASTokenProvider.class.getCanonicalName()).isEqualTo(sasProvider);

    assumeThat(this.getAuthType()).isEqualTo(AuthType.SharedKey);
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    Boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    if (!isHNSEnabled) {
      assumeBlobServiceType();
    }

    //todo: thisi is changed
    createFilesystemForUserBoundSASTests();
    super.setup();
  }
 // TEST FOR SAS- HOW DOES CREATE CONTAINER PASS!!!


  // Common helper to inject a mock token provider into AbfsClient
  private void injectMockTokenProvider(AzureBlobFileSystem fs, AccessTokenProvider mockProvider) throws Exception {
    Field abfsStoreField = AzureBlobFileSystem.class.getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    AzureBlobFileSystemStore store = (AzureBlobFileSystemStore) abfsStoreField.get(fs);

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField("client");
    abfsClientField.setAccessible(true);
    AbfsClient client = (AbfsClient) abfsClientField.get(store);

    Field tokenProviderField = AbfsClient.class.getDeclaredField("tokenProvider");
    tokenProviderField.setAccessible(true);
    tokenProviderField.set(client, mockProvider);
  }

  private void addOAuthConfigs(AzureBlobFileSystem fs, Configuration testConfig, String accountName) {
    AbfsConfiguration abfsConfig = fs.getAbfsStore().getAbfsConfiguration();

    testConfig.set(FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID + "." +accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));
    testConfig.set(FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));

    testConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));
    testConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));

    testConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET+ "." + accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));
    testConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));

    // Set a different SDUOID
    testConfig.set(FS_AZURE_END_USER_OBJECT_ID, abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));
//    testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
//        "org.apache.hadoop.fs.azurebfs.extensions.MockUserBoundSASTokenProvider");
  }


  @Test
  public void testShouldFailWhenSduoidMismatchesServicePrincipalId()
    //WIHOUT ABSTRACT INTEGRATION CLASS
      throws Exception {
    // Arrange
        //final AzureBlobFileSystem fs = this.getFileSystem();
        AzureBlobFileSystem fs = getFileSystem();
        //AzureBlobFileSystem fs = createFs();
        AzureBlobFileSystem testFs = new AzureBlobFileSystem();

        String accountName = getAccountName();

//        Configuration testConfig = getConfiguration()
        Configuration testConfig = new Configuration(getRawConfiguration());
        addOAuthConfigs(fs, testConfig, accountName);

        testConfig.set(FS_AZURE_END_USER_OBJECT_ID, TEST_OBJECT_ID);
        testFs.initialize(fs.getUri(), testConfig);
        intercept(AccessDeniedException.class,
            ()-> {
              testFs.create(testPath);
            });
  }

  @Test
  public void testReadAndWrite() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    String accountName = getAccountName();

//        Configuration testConfig = getConfiguration()
    Configuration testConfig = new Configuration(getRawConfiguration());
   // addOAuthConfigs(fs, testConfig, accountName);
    testFs.initialize(fs.getUri(), testConfig);

    Path reqPath = new Path(UUID.randomUUID().toString());

    final String msg1 = "purple";
    final String msg2 = "yellow";
    int expectedFileLength = msg1.length() * 2;

    byte[] readBuffer = new byte[1024];

    // create file with content "purplepurple"
    try (FSDataOutputStream stream = testFs.create(reqPath)) {
      stream.writeBytes(msg1);
      stream.hflush();
      stream.writeBytes(msg1);
    }

    // open file and verify content is "purplepurple"
    try (FSDataInputStream stream = testFs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg1 + msg1, fileContent);
    }

    // overwrite file with content "yellowyellow"
    try (FSDataOutputStream stream = testFs.create(reqPath)) {
      stream.writeBytes(msg2);
      stream.hflush();
      stream.writeBytes(msg2);
    }

    // open file and verify content is "yellowyellow"
    try (FSDataInputStream stream = testFs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg2 + msg2, fileContent);
    }

    // append to file so final content is "yellowyellowpurplepurple"
    try (FSDataOutputStream stream = testFs.append(reqPath)) {
      stream.writeBytes(msg1);
      stream.hflush();
      stream.writeBytes(msg1);
    }

    // open file and verify content is "yellowyellowpurplepurple"
    try (FSDataInputStream stream = testFs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(2 * expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg2 + msg2 + msg1 + msg1, fileContent);
    }
  }


  @Test
  // Verify OAuth token provider and user-bound SAS provider are both configured and usable
  public void testOAuthTokenProviderAndSASTokenFlow() throws Exception {
     AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    String accountName = getAccountName();
    Configuration testConfig = new Configuration(getRawConfiguration());
    addOAuthConfigs(fs, testConfig, accountName);
    testFs.initialize(fs.getUri(), testConfig);

    // Verify AbfsConfiguration has an OAuth token provider configured
    AbfsConfiguration abfsConfiguration = testFs.getAbfsStore().getAbfsConfiguration();

    AccessTokenProvider tokenProvider = abfsConfiguration.getTokenProvider();
    assertNotNull(tokenProvider, "AccessTokenProvider must be configured for UserboundSASWithOAuth");

    // Acquire an OAuth token and assert it is non-empty
    AzureADToken token = tokenProvider.getToken();
    assertNotNull(token, "OAuth token must not be null");
    assertNotNull(token.getAccessToken(), "OAuth access token must not be null");
    assertFalse(token.getAccessToken().isEmpty(), "OAuth access token must not be empty");

    // Verify SASTokenProvider for user-bound SAS is present and usable
    SASTokenProvider sasProvider = abfsConfiguration.getSASTokenProviderForUserBoundSAS();
    assertNotNull(sasProvider, "SASTokenProvider for user-bound SAS must be configured");
    assertTrue(sasProvider instanceof MockUserBoundSASTokenProvider,
        "Expected MockUserBoundSASTokenProvider to be used for tests");

    // Request a SAS token and assert we get a non-empty result
    String sasToken = sasProvider.getSASToken("abfsdrivercanaryhns.dfs.core.windows.net", "userbound", "/", SASTokenProvider.GET_PROPERTIES_OPERATION);
    assertNotNull(sasToken, "SAS token must not be null");
    assertFalse(sasToken.isEmpty(), "SAS token must not be empty");
  }

  @Test
  public void testOpenFile() throws Exception {
    AzureBlobFileSystem fs = getFileSystem(getRawConfiguration()); //dont change
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    AbfsConfiguration abfsConfig = fs.getAbfsStore().getAbfsConfiguration();

    String accountName = getAccountName();

    Configuration testConfig = new Configuration(getRawConfiguration()); //dont change
  //  Configuration testConfig = fs.getAbfsStore().getAbfsConfiguration().getRawConfiguration();

    addOAuthConfigs(fs, testConfig, accountName);
    testFs.initialize(fs.getUri(), testConfig);

//    System.out.print(testFs.getAbfsStore().getAbfsConfiguration());
//    System.out.print(abfsConfig);

    testFs.create(testPath).close();
    testFs.open(testPath);
    testFs.getFileStatus(testPath);
  }


  @Test
  public void testReadWriteFailsWithInvalidOAuthToken() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    String accountName = getAccountName();

    Configuration testConfig = new Configuration(getRawConfiguration());
    addOAuthConfigs(fs, testConfig, accountName);

    testFs.initialize(fs.getUri(), testConfig);

    // Create mock token provider with invalid token
    AccessTokenProvider mockProvider = Mockito.mock(AccessTokenProvider.class);
    AzureADToken mockToken = Mockito.mock(AzureADToken.class);
    Mockito.when(mockToken.getAccessToken()).thenReturn("1234=abcd"); // Invalid token
    Mockito.when(mockProvider.getToken()).thenReturn(mockToken);

    // Inject mock provider into AbfsClient
    injectMockTokenProvider(testFs, mockProvider);

    intercept(AccessDeniedException.class, () -> {testFs.create(testPath); });
  }

  @Test
  public void testReadWriteFailsWithInvalidSASToken() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    String accountName = getAccountName();

    Configuration testConfig = new Configuration(getRawConfiguration());
    addOAuthConfigs(fs, testConfig, accountName);
    testConfig.unset("fs.azure.sas.token.provider.type");
    testConfig.set("fs.azure.sas.token.provider.type", "org.apache.hadoop.fs.azurebfs.extensions.MockInvalidSASTokenProvider");

    testFs.initialize(fs.getUri(), testConfig);

    intercept(AccessDeniedException.class, () -> {testFs.create(testPath); });
  }

  @Test
  public void testOperationWithValidAndExpiredSASToken() throws Exception {
    // Set up the Configuration and FileSystem
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystem testFs = new AzureBlobFileSystem();

    Configuration testConfig = new Configuration(getRawConfiguration());
    addOAuthConfigs(fs, testConfig, getAccountName());

    testFs.initialize(fs.getUri(), testConfig);

    // Get a real SAS token from the configured provider
    AbfsConfiguration abfsConfig = testFs.getAbfsStore().getAbfsConfiguration();
    SASTokenProvider realSasProvider = abfsConfig.getSASTokenProviderForUserBoundSAS();
    assertNotNull(realSasProvider, "SASTokenProvider for user-bound SAS must be configured");
    String validSasToken = realSasProvider.getSASToken(
        getAccountName(),
        testFs.toString(),
        String.valueOf(testPath),
        SASTokenProvider.GET_PROPERTIES_OPERATION);
    assertNotNull(validSasToken, "SAS token must not be null");
    assertFalse(validSasToken.isEmpty(), "SAS token must not be empty");

    // 1. Operation should work with valid SAS token
    // (No exception expected)
    org.apache.hadoop.fs.Path path = testPath;
    testFs.create(path); // Should succeed

    // 2. Now, modify the ske/se fields to be expired and inject a mock provider
    String expiredDate = OffsetDateTime.now(ZoneOffset.UTC)
        .minusDays(1)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    String expiredSasToken = java.util.Arrays.stream(validSasToken.split("&"))
        .map(kv -> {
          String[] pair = kv.split("=", 2);
          if (pair[0].equals("ske") || pair[0].equals("se")) {
            return pair[0] + "=" + expiredDate;
          } else {
            return kv;
          }
        })
        .collect(java.util.stream.Collectors.joining("&"));

    // Create a mock SASTokenProvider that returns the expired SAS token
    SASTokenProvider mockSasProvider = org.mockito.Mockito.mock(
        SASTokenProvider.class);
    org.mockito.Mockito.when(
            mockSasProvider.getSASToken(org.mockito.Mockito.anyString(),
                org.mockito.Mockito.anyString(), org.mockito.Mockito.anyString(),
                org.mockito.Mockito.anyString()))
        .thenReturn(expiredSasToken);

    // Inject the mock provider into the AbfsClient
    injectMockSASTokenProvider(testFs, mockSasProvider);

    // Try a file operation and expect failure due to expired SAS token
    intercept(AccessDeniedException.class, () -> {testFs.getFileStatus(path);});
  }

  // Helper to inject a mock SASTokenProvider into the AbfsClient
  private void injectMockSASTokenProvider(AzureBlobFileSystem fs, SASTokenProvider provider) throws Exception {
    Field abfsStoreField = AzureBlobFileSystem.class.getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    AzureBlobFileSystemStore store = (AzureBlobFileSystemStore) abfsStoreField.get(fs);

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField("client");
    abfsClientField.setAccessible(true);
    AbfsClient client = (AbfsClient) abfsClientField.get(store);

    // Use AbfsClient.class to get the field, not client.getClass()
    java.lang.reflect.Field sasProviderField = AbfsClient.class.getDeclaredField("sasTokenProvider");
    sasProviderField.setAccessible(true);
    sasProviderField.set(client, provider);
  }
}
