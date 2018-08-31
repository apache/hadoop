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

package org.apache.hadoop.fs.azure;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assume.assumeNotNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.EnumSet;
import java.io.File;

import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.CreateOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class ITestWasbUriAndConfiguration extends AbstractWasbTestWithTimeout {

  private static final int FILE_SIZE = 4096;
  private static final String PATH_DELIMITER = "/";

  protected String accountName;
  protected String accountKey;
  protected static Configuration conf = null;
  private boolean runningInSASMode = false;
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private AzureBlobStorageTestAccount testAccount;

  @After
  public void tearDown() throws Exception {
    testAccount = AzureTestUtils.cleanupTestAccount(testAccount);
  }

  @Before
  public void setMode() {
    runningInSASMode = AzureBlobStorageTestAccount.createTestConfiguration().
        getBoolean(AzureNativeFileSystemStore.KEY_USE_SECURE_MODE, false);
  }

  private boolean validateIOStreams(Path filePath) throws IOException {
    // Capture the file system from the test account.
    FileSystem fs = testAccount.getFileSystem();
    return validateIOStreams(fs, filePath);
  }

  private boolean validateIOStreams(FileSystem fs, Path filePath)
      throws IOException {

    // Create and write a file
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();

    // Return true if the the count is equivalent to the file size.
    return (FILE_SIZE == readInputStream(fs, filePath));
  }

  private int readInputStream(Path filePath) throws IOException {
    // Capture the file system from the test account.
    FileSystem fs = testAccount.getFileSystem();
    return readInputStream(fs, filePath);
  }

  private int readInputStream(FileSystem fs, Path filePath) throws IOException {
    // Read the file
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();

    // Return true if the the count is equivalent to the file size.
    return count;
  }

  // Positive tests to exercise making a connection with to Azure account using
  // account key.
  @Test
  public void testConnectUsingKey() throws Exception {

    testAccount = AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);

    // Validate input and output on the connection.
    assertTrue(validateIOStreams(new Path("/wasb_scheme")));
  }

  @Test
  public void testConnectUsingSAS() throws Exception {

    Assume.assumeFalse(runningInSASMode);
    // Create the test account with SAS credentials.
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas, CreateOptions.CreateContainer));
    assumeNotNull(testAccount);
    // Validate input and output on the connection.
    // NOTE: As of 4/15/2013, Azure Storage has a deficiency that prevents the
    // full scenario from working (CopyFromBlob doesn't work with SAS), so
    // just do a minor check until that is corrected.
    assertFalse(testAccount.getFileSystem().exists(new Path("/IDontExist")));
    //assertTrue(validateIOStreams(new Path("/sastest.txt")));
  }

  @Test
  public void testConnectUsingSASReadonly() throws Exception {

    Assume.assumeFalse(runningInSASMode);
    // Create the test account with SAS credentials.
    testAccount = AzureBlobStorageTestAccount.create("", EnumSet.of(
        CreateOptions.UseSas, CreateOptions.CreateContainer,
        CreateOptions.Readonly));
    assumeNotNull(testAccount);

    // Create a blob in there
    final String blobKey = "blobForReadonly";
    CloudBlobContainer container = testAccount.getRealContainer();
    CloudBlockBlob blob = container.getBlockBlobReference(blobKey);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[] { 1,
        2, 3 });
    blob.upload(inputStream, 3);
    inputStream.close();

    // Make sure we can read it from the file system
    Path filePath = new Path("/" + blobKey);
    FileSystem fs = testAccount.getFileSystem();
    assertTrue(fs.exists(filePath));
    byte[] obtained = new byte[3];
    DataInputStream obtainedInputStream = fs.open(filePath);
    obtainedInputStream.readFully(obtained);
    obtainedInputStream.close();
    assertEquals(3, obtained[2]);
  }

  @Test
  public void testConnectUsingAnonymous() throws Exception {

    // Create test account with anonymous credentials
    testAccount = AzureBlobStorageTestAccount.createAnonymous("testWasb.txt",
        FILE_SIZE);
    assumeNotNull(testAccount);

    // Read the file from the public folder using anonymous credentials.
    assertEquals(FILE_SIZE, readInputStream(new Path("/testWasb.txt")));
  }

  @Test
  public void testConnectToEmulator() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createForEmulator();
    assumeNotNull(testAccount);
    assertTrue(validateIOStreams(new Path("/testFile")));
  }

  /**
   * Tests that we can connect to fully qualified accounts outside of
   * blob.core.windows.net
   */
  @Test
  public void testConnectToFullyQualifiedAccountMock() throws Exception {
    Configuration conf = new Configuration();
    AzureBlobStorageTestAccount.setMockAccountKey(conf,
        "mockAccount.mock.authority.net");
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    NativeAzureFileSystem fs = new NativeAzureFileSystem(store);
    fs.initialize(
        new URI("wasb://mockContainer@mockAccount.mock.authority.net"), conf);
    fs.createNewFile(new Path("/x"));
    assertTrue(mockStorage.getBackingStore().exists(
        "http://mockAccount.mock.authority.net/mockContainer/x"));
    fs.close();
  }

  public void testConnectToRoot() throws Exception {

    // Set up blob names.
    final String blobPrefix = String.format("wasbtests-%s-%tQ-blob",
        System.getProperty("user.name"), new Date());
    final String inblobName = blobPrefix + "_In" + ".txt";
    final String outblobName = blobPrefix + "_Out" + ".txt";

    // Create test account with default root access.
    testAccount = AzureBlobStorageTestAccount.createRoot(inblobName, FILE_SIZE);
    assumeNotNull(testAccount);

    // Read the file from the default container.
    assertEquals(FILE_SIZE, readInputStream(new Path(PATH_DELIMITER
        + inblobName)));

    try {
      // Capture file system.
      FileSystem fs = testAccount.getFileSystem();

      // Create output path and open an output stream to the root folder.
      Path outputPath = new Path(PATH_DELIMITER + outblobName);
      OutputStream outputStream = fs.create(outputPath);
      fail("Expected an AzureException when writing to root folder.");
      outputStream.write(new byte[FILE_SIZE]);
      outputStream.close();
    } catch (AzureException e) {
      assertTrue(true);
    } catch (Exception e) {
      String errMsg = String.format(
          "Expected AzureException but got %s instead.", e);
      assertTrue(errMsg, false);
    }
  }

  // Positive tests to exercise throttling I/O path. Connections are made to an
  // Azure account using account key.
  //
  public void testConnectWithThrottling() throws Exception {

    testAccount = AzureBlobStorageTestAccount.createThrottled();

    // Validate input and output on the connection.
    assertTrue(validateIOStreams(new Path("/wasb_scheme")));
  }

  /**
   * Creates a file and writes a single byte with the given value in it.
   */
  private static void writeSingleByte(FileSystem fs, Path testFile, int toWrite)
      throws Exception {
    OutputStream outputStream = fs.create(testFile);
    outputStream.write(toWrite);
    outputStream.close();
  }

  /**
   * Reads the file given and makes sure that it's a single-byte file with the
   * given value in it.
   */
  private static void assertSingleByteValue(FileSystem fs, Path testFile,
      int expectedValue) throws Exception {
    InputStream inputStream = fs.open(testFile);
    int byteRead = inputStream.read();
    assertTrue("File unexpectedly empty: " + testFile, byteRead >= 0);
    assertTrue("File has more than a single byte: " + testFile,
        inputStream.read() < 0);
    inputStream.close();
    assertEquals("Unxpected content in: " + testFile, expectedValue, byteRead);
  }

  @Test
  public void testMultipleContainers() throws Exception {
    AzureBlobStorageTestAccount firstAccount = AzureBlobStorageTestAccount
        .create("first"), secondAccount = AzureBlobStorageTestAccount
        .create("second");
    assumeNotNull(firstAccount);
    assumeNotNull(secondAccount);
    try {
      FileSystem firstFs = firstAccount.getFileSystem(),
          secondFs = secondAccount.getFileSystem();
      Path testFile = new Path("/testWasb");
      assertTrue(validateIOStreams(firstFs, testFile));
      assertTrue(validateIOStreams(secondFs, testFile));
      // Make sure that we're really dealing with two file systems here.
      writeSingleByte(firstFs, testFile, 5);
      writeSingleByte(secondFs, testFile, 7);
      assertSingleByteValue(firstFs, testFile, 5);
      assertSingleByteValue(secondFs, testFile, 7);
    } finally {
      firstAccount.cleanup();
      secondAccount.cleanup();
    }
  }

  @Test
  public void testDefaultKeyProvider() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "testkey";

    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);

    String result = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
        account, conf);
    assertEquals(key, result);
  }

  @Test
  public void testCredsFromCredentialProvider() throws Exception {

    Assume.assumeFalse(runningInSASMode);
    String account = "testacct";
    String key = "testkey";
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccountKey(conf, account, key);

    // also add to configuration as clear text that should be overridden
    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account,
        key + "cleartext");

    String result = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
        account, conf);
    // result should contain the credential provider key not the config key
    assertEquals("AccountKey incorrect.", key, result);
  }

  void provisionAccountKey(
      final Configuration conf, String account, String key) throws Exception {
    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(
        SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key.toCharArray());
    provider.flush();
  }

  @Test
  public void testValidKeyProvider() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "testkey";

    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);
    conf.setClass("fs.azure.account.keyprovider." + account,
        SimpleKeyProvider.class, KeyProvider.class);
    String result = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
        account, conf);
    assertEquals(key, result);
  }

  @Test
  public void testInvalidKeyProviderNonexistantClass() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";

    conf.set("fs.azure.account.keyprovider." + account,
        "org.apache.Nonexistant.Class");
    try {
      AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
      Assert.fail("Nonexistant key provider class should have thrown a "
          + "KeyProviderException");
    } catch (KeyProviderException e) {
    }
  }

  @Test
  public void testInvalidKeyProviderWrongClass() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";

    conf.set("fs.azure.account.keyprovider." + account, "java.lang.String");
    try {
      AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
      Assert.fail("Key provider class that doesn't implement KeyProvider "
          + "should have thrown a KeyProviderException");
    } catch (KeyProviderException e) {
    }
  }

  /**
   * Tests the cases when the URI is specified with no authority, i.e.
   * wasb:///path/to/file.
   */
  @Test
  public void testNoUriAuthority() throws Exception {
    // For any combination of default FS being asv(s)/wasb(s)://c@a/ and
    // the actual URI being asv(s)/wasb(s):///, it should work.

    String[] wasbAliases = new String[] { "wasb", "wasbs" };
    for (String defaultScheme : wasbAliases) {
      for (String wantedScheme : wasbAliases) {
        testAccount = AzureBlobStorageTestAccount.createMock();
        Configuration conf = testAccount.getFileSystem().getConf();
        String authority = testAccount.getFileSystem().getUri().getAuthority();
        URI defaultUri = new URI(defaultScheme, authority, null, null, null);
        conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
        // Add references to file system implementations for wasb and wasbs.
        conf.addResource("azure-test.xml");
        URI wantedUri = new URI(wantedScheme + ":///random/path");
        NativeAzureFileSystem obtained = (NativeAzureFileSystem) FileSystem
            .get(wantedUri, conf);
        assertNotNull(obtained);
        assertEquals(new URI(wantedScheme, authority, null, null, null),
            obtained.getUri());
        // Make sure makeQualified works as expected
        Path qualified = obtained.makeQualified(new Path(wantedUri));
        assertEquals(new URI(wantedScheme, authority, wantedUri.getPath(),
            null, null), qualified.toUri());
        // Cleanup for the next iteration to not cache anything in FS
        testAccount.cleanup();
        FileSystem.closeAll();
      }
    }
    // If the default FS is not a WASB FS, then specifying a URI without
    // authority for the Azure file system should throw.
    testAccount = AzureBlobStorageTestAccount.createMock();
    Configuration conf = testAccount.getFileSystem().getConf();
    conf.set(FS_DEFAULT_NAME_KEY, "file:///");
    try {
      FileSystem.get(new URI("wasb:///random/path"), conf);
      fail("Should've thrown.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testWasbAsDefaultFileSystemHasNoPort() throws Exception {
    try {
      testAccount = AzureBlobStorageTestAccount.createMock();
      Configuration conf = testAccount.getFileSystem().getConf();
      String authority = testAccount.getFileSystem().getUri().getAuthority();
      URI defaultUri = new URI("wasb", authority, null, null, null);
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      conf.addResource("azure-test.xml");

      FileSystem fs = FileSystem.get(conf);
      assertTrue(fs instanceof NativeAzureFileSystem);
      assertEquals(-1, fs.getUri().getPort());

      AbstractFileSystem afs = FileContext.getFileContext(conf)
          .getDefaultFileSystem();
      assertTrue(afs instanceof Wasb);
      assertEquals(-1, afs.getUri().getPort());
    } finally {
      testAccount.cleanup();
      FileSystem.closeAll();
    }
  }

   /**
   * Tests the cases when the scheme specified is 'wasbs'.
   */
  @Test
  public void testAbstractFileSystemImplementationForWasbsScheme() throws Exception {
    try {
      testAccount = AzureBlobStorageTestAccount.createMock();
      Configuration conf = testAccount.getFileSystem().getConf();
      String authority = testAccount.getFileSystem().getUri().getAuthority();
      URI defaultUri = new URI("wasbs", authority, null, null, null);
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");
      conf.addResource("azure-test.xml");

      FileSystem fs = FileSystem.get(conf);
      assertTrue(fs instanceof NativeAzureFileSystem);
      assertEquals("wasbs", fs.getScheme());

      AbstractFileSystem afs = FileContext.getFileContext(conf)
          .getDefaultFileSystem();
      assertTrue(afs instanceof Wasbs);
      assertEquals(-1, afs.getUri().getPort());
      assertEquals("wasbs", afs.getUri().getScheme());
    } finally {
      testAccount.cleanup();
      FileSystem.closeAll();
    }
  }

  @Test
  public void testCredentialProviderPathExclusions() throws Exception {
    String providerPath =
        "user:///,jceks://wasb/user/hrt_qa/sqoopdbpasswd.jceks," +
        "jceks://hdfs@nn1.example.com/my/path/test.jceks";
    Configuration config = new Configuration();
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        providerPath);
    String newPath = "user:///,jceks://hdfs@nn1.example.com/my/path/test.jceks";

    excludeAndTestExpectations(config, newPath);
  }

  @Test
  public void testExcludeAllProviderTypesFromConfig() throws Exception {
    String providerPath =
        "jceks://wasb/tmp/test.jceks," +
        "jceks://wasb@/my/path/test.jceks";
    Configuration config = new Configuration();
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        providerPath);
    String newPath = null;

    excludeAndTestExpectations(config, newPath);
  }

  void excludeAndTestExpectations(Configuration config, String newPath)
    throws Exception {
    Configuration conf = ProviderUtils.excludeIncompatibleCredentialProviders(
        config, NativeAzureFileSystem.class);
    String effectivePath = conf.get(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, null);
    assertEquals(newPath, effectivePath);
  }

  @Test
  public void testUserAgentConfig() throws Exception {
    // Set the user agent
    try {
      testAccount = AzureBlobStorageTestAccount.createMock();
      Configuration conf = testAccount.getFileSystem().getConf();
      String authority = testAccount.getFileSystem().getUri().getAuthority();
      URI defaultUri = new URI("wasbs", authority, null, null, null);
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");

      conf.set(AzureNativeFileSystemStore.USER_AGENT_ID_KEY, "TestClient");

      FileSystem fs = FileSystem.get(conf);
      AbstractFileSystem afs = FileContext.getFileContext(conf).getDefaultFileSystem();

      assertTrue(afs instanceof Wasbs);
      assertEquals(-1, afs.getUri().getPort());
      assertEquals("wasbs", afs.getUri().getScheme());

    } finally {
      testAccount.cleanup();
      FileSystem.closeAll();
    }

    // Unset the user agent
    try {
      testAccount = AzureBlobStorageTestAccount.createMock();
      Configuration conf = testAccount.getFileSystem().getConf();
      String authority = testAccount.getFileSystem().getUri().getAuthority();
      URI defaultUri = new URI("wasbs", authority, null, null, null);
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");

      conf.unset(AzureNativeFileSystemStore.USER_AGENT_ID_KEY);

      FileSystem fs = FileSystem.get(conf);
      AbstractFileSystem afs = FileContext.getFileContext(conf).getDefaultFileSystem();
      assertTrue(afs instanceof Wasbs);
      assertEquals(-1, afs.getUri().getPort());
      assertEquals("wasbs", afs.getUri().getScheme());

    } finally {
      testAccount.cleanup();
      FileSystem.closeAll();
    }
  }
}
