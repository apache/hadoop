/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import com.fasterxml.jackson.dataformat.xml.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurebfs.constants.*;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.extensions.*;
import org.apache.hadoop.fs.azurebfs.oauth2.*;
import org.apache.hadoop.fs.permission.*;
import org.codehaus.jettison.json.*;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.junit.runners.*;

import java.io.*;
import java.net.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer.*;
import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizerEnums.*;
import static org.apache.hadoop.fs.azurebfs.sasTokenGenerator.SASTokenConstants.VALID_SAS_REST_API_VERSIONS;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;


/**
 * Test Perform Authorization Check operation
 */
@RunWith(Parameterized.class)
public class ITestAzureBlobFileSystemAuthorization
    extends AbstractAbfsIntegrationTest {
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();
  private final String test_authorize_class;

  @Rule
  public TestName name = new TestName();

  @Parameterized.Parameters(name = "test_authorize_class={0}")
  public static Iterable<Object> getMockAuthorizer() {
    return Arrays.asList(
        //"org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer",
        "org.apache.hadoop.fs.azurebfs.extensions.MockAbfsSASAuthorizer");
  }

  public ITestAzureBlobFileSystemAuthorization(final String authorizerClassName) throws Exception {
    test_authorize_class = authorizerClassName;
    if (test_authorize_class.equals("org.apache.hadoop.fs.azurebfs.extensions"
        + ".MockAbfsSASAuthorizer")) {
      // Test enabled to fetch delegation key using oAuth creds
      Assume.assumeTrue(this.getConfiguration().getTokenProvider() != null);

      MockAbfsSASAuthorizer.setUserDelegationKey(fetchDelegationKey(this.getConfiguration().getTokenProvider(),
          this.getConfiguration().getAccountName()));
    }
  }

  @Override
  public void setup() throws Exception {
    boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(isHNSEnabled);
    this.getConfiguration().setAbfsAuthorizerClass(test_authorize_class);
    loadAuthorizer();
    super.setup();
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class, () -> {
      fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    runTest(FileSystemOperations.Open, false);
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.Open, true);
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, false);
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, true);
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    runTest(FileSystemOperations.AppendClose,
        false);
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.AppendClose,
        true);
  }

  @Test
  public void testRenameSourceUnauthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, true);
  }

  @Test
  public void testRenameDestUnauthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, true);
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath,
        false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath, true);
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, false);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, true);
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, false);
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, true);
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.GetPathStatus,
        false);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.GetPathStatus,
        true);
  }

  @Test
  public void testSetOwnerAuthorized() throws Exception {
    runTest(FileSystemOperations.SetOwner, false);
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    runTest(FileSystemOperations.SetOwner, true);
  }

  @Test
  public void testSetPermissionAuthorized() throws Exception {
    runTest(FileSystemOperations.SetPermissions,
        false);
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    runTest(FileSystemOperations.SetPermissions,
        true);
  }

  @Test
  public void testModifyAclEntriesAuthorized() throws Exception {
    runTest(FileSystemOperations.ModifyAclEntries,
        false);
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
       runTest(FileSystemOperations.ModifyAclEntries,
        true);
  }

  @Test
  public void testRemoveAclEntriesAuthorized() throws Exception {
    runTest(FileSystemOperations.RemoveAclEntries,
        false);
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    runTest(FileSystemOperations.RemoveAclEntries,
        true);
  }

  @Test
  public void testRemoveDefaultAclAuthorized() throws Exception {
    runTest(FileSystemOperations.RemoveDefaultAcl,
        false);
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    runTest(FileSystemOperations.RemoveDefaultAcl,
        true);
  }

  @Test
  public void testRemoveAclAuthorized() throws Exception {
    runTest(FileSystemOperations.RemoveAcl, false);
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    runTest(FileSystemOperations.RemoveAcl, true);
  }

  @Test
  public void testSetAclAuthorized() throws Exception {
    runTest(FileSystemOperations.SetAcl, false);
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
       runTest(FileSystemOperations.SetAcl, true);
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.GetAcl, false);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.GetAcl, true);
  }

  private void runTest(FileSystemOperations testOp,
      boolean expectAbfsAuthorizationException) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path reqPath = new Path("requestPath"
        + UUID.randomUUID().toString()
        + (expectAbfsAuthorizationException ? "unauthorized":""));

    getMockAuthorizer(fs).setSkipAuthCheck(true);
    if ((testOp != FileSystemOperations.CreatePath)
    && (testOp != FileSystemOperations.Mkdir))
    {
      fs.create(reqPath).close();
    }
    getMockAuthorizer(fs).setSkipAuthCheck(false);

    // Test Operation
    if (expectAbfsAuthorizationException) {
      intercept(AbfsAuthorizationException.class, () -> {
        executeOp(reqPath, fs, testOp);
      });
    } else {
      executeOp(reqPath, fs, testOp);
    }
  }

  private void executeOp(Path reqPath, AzureBlobFileSystem fs,
      FileSystemOperations op)
      throws IOException {


    switch (op) {
    case ListPaths:
      fs.listStatus(reqPath);
      break;
    case CreatePath:
      fs.create(reqPath);
      break;
    case RenamePath:
      fs.rename(reqPath,
          new Path("renameDest" + UUID.randomUUID().toString()));
      break;
    case GetAcl:
      fs.getAclStatus(reqPath);
      break;
    case GetPathStatus:
      fs.getFileStatus(reqPath);
      break;
    case SetAcl:
      fs.setAcl(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case SetOwner:
      fs.setOwner(reqPath, TEST_USER, TEST_GROUP);
      break;
    case SetPermissions:
      fs.setPermission(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case Append:
      fs.append(reqPath);
      break;
    case ReadFile:
      fs.open(reqPath);
      break;
    case AppendClose:
      fs.append(reqPath).close();
      break;
    case CreateClose:
      fs.create(reqPath).close();
      break;
    case Open:
      fs.open(reqPath);
      break;
    case DeletePath:
      fs.delete(reqPath, false);
      break;
    case Mkdir:
      fs.mkdirs(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case RemoveAclEntries:
      fs.removeAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case ModifyAclEntries:
      fs.modifyAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case RemoveAcl:
      fs.removeAcl(reqPath);
      break;
    case RemoveDefaultAcl:
      fs.removeDefaultAcl(reqPath);
      break;
    default:
      throw new IllegalStateException("Unexpected value: " + op);
    }
  }

  private MockAbfsAuthorizer getMockAuthorizer(AzureBlobFileSystem fs)
      throws Exception {
    return ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer());
  }

  public static final DateTimeFormatter ISO_8601_UTC_DATE_FORMATTER =
      DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT)
          .withZone(ZoneId.of("UTC"));

  public static UserDelegationKey fetchDelegationKey(AccessTokenProvider oauthTokenProvider,
      String accountName) throws IOException, JSONException {
    String accessToken = oauthTokenProvider.getToken().getAccessToken();
    String restVersion = VALID_SAS_REST_API_VERSIONS.iterator().next();
    String start = ISO_8601_UTC_DATE_FORMATTER.format(Instant.now());
    String expiry = ISO_8601_UTC_DATE_FORMATTER
        .format(Instant.now().plusSeconds(10 * 60));

    HttpURLConnection conn = null;
    String urlString = "https://" + accountName.replace("dfs", "blob")
        + "/?restype=service&comp=userdelegationkey";
    String httpMethod = "POST";
    URL url = new URL(urlString);
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(httpMethod);
    conn.setRequestProperty("x-ms-version", restVersion);
    conn.setRequestProperty("Authorization", "Bearer " + accessToken);
    conn.setRequestProperty("Content-Type", "application/xml;charset=utf-8");
    conn.setDoOutput(true);

    KeyInfo keyInfo = new KeyInfo();
    keyInfo.Start = start;
    keyInfo.Expiry = expiry;
    XmlMapper mapper = new XmlMapper();
    String content = mapper.writeValueAsString(keyInfo);
    conn.getOutputStream().write(content.getBytes("UTF-8"));
    BufferedReader in = null;
    try {
      conn.getResponseCode();
      in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    } catch (Exception ex) {
      System.out.println(" exception: " + ex.getMessage() + ex.getStackTrace());
    }
    String inputLine;
    StringBuffer response = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();
    //print in String
    System.out.println(response.toString());
    return mapper
        .readValue(response.toString(), UserDelegationKey.class);
  }

  enum FileSystemOperations {
    None, ListPaths, CreatePath, RenamePath, GetAcl, GetPathStatus, SetAcl,
    SetOwner, SetPermissions, Append, ReadFile, DeletePath, Mkdir,
    RemoveAclEntries, RemoveDefaultAcl, RemoveAcl, ModifyAclEntries,
    AppendClose, CreateClose, Open
  }
}
