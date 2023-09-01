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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_CORRELATIONID;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;

public class TestTracingContext extends AbstractAbfsIntegrationTest {
  private static final String[] CLIENT_CORRELATIONID_LIST = {
      "valid-corr-id-123", "inval!d", ""};
  private static final int HTTP_CREATED = 201;

  public TestTracingContext() throws Exception {
    super();
  }

  @Test
  public void testClientCorrelationId() throws Exception {
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[0], true);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[1], false);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[2], false);
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String
        .format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }

  public void checkCorrelationConfigValidation(String clientCorrelationId,
      boolean includeInHeader) throws Exception {
    Configuration conf = getRawConfiguration();
    conf.set(FS_AZURE_CLIENT_CORRELATIONID, clientCorrelationId);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(conf);

    String correlationID = fs.getClientCorrelationId();
    if (includeInHeader) {
      Assertions.assertThat(correlationID)
          .describedAs("Correlation ID should match config when valid")
          .isEqualTo(clientCorrelationId);
    } else {
      Assertions.assertThat(correlationID)
          .describedAs("Invalid ID should be replaced with empty string")
          .isEqualTo(EMPTY_STRING);
    }
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fs.getFileSystemId(), FSOperationType.TEST_OP,
        TracingHeaderFormat.ALL_ID_FORMAT, null);
    boolean isNamespaceEnabled = fs.getIsNamespaceEnabled(tracingContext);
    String path = getRelativePath(new Path("/testDir"));
    String permission = isNamespaceEnabled
        ? getOctalNotation(FsPermission.getDirDefault())
        : null;
    String umask = isNamespaceEnabled
        ? getOctalNotation(FsPermission.getUMask(fs.getConf()))
        : null;

    //request should not fail for invalid clientCorrelationID
    AbfsRestOperation op = fs.getAbfsClient()
        .createPath(path, false, true, permission, umask, false, null,
            tracingContext);

    int statusCode = op.getResult().getStatusCode();
    Assertions.assertThat(statusCode).describedAs("Request should not fail")
        .isEqualTo(HTTP_CREATED);

    String requestHeader = op.getResult().getClientRequestId().replace("[", "")
        .replace("]", "");
    Assertions.assertThat(requestHeader)
        .describedAs("Client Request Header should match TracingContext")
        .isEqualTo(op.getLastTracingContext().getHeader());
  }

  @Ignore
  @Test
  //call test methods from the respective test classes
  //can be ignored when running all tests as these get covered
  public void runCorrelationTestForAllMethods() throws Exception {
    Map<AbstractAbfsIntegrationTest, Method> testClasses = new HashMap<>();

    testClasses.put(new ITestAzureBlobFileSystemListStatus(), //liststatus
        ITestAzureBlobFileSystemListStatus.class.getMethod("testListPath"));
    testClasses.put(new ITestAbfsReadWriteAndSeek(MIN_BUFFER_SIZE, true), //open,
        // read, write
        ITestAbfsReadWriteAndSeek.class.getMethod("testReadAheadRequestID"));
    testClasses.put(new ITestAbfsReadWriteAndSeek(MIN_BUFFER_SIZE, false), //read (bypassreadahead)
        ITestAbfsReadWriteAndSeek.class
            .getMethod("testReadAndWriteWithDifferentBufferSizesAndSeek"));
    testClasses.put(new ITestAzureBlobFileSystemAppend(), //append
        ITestAzureBlobFileSystemAppend.class.getMethod("testTracingForAppend"));
    testClasses.put(new ITestAzureBlobFileSystemFlush(),
        ITestAzureBlobFileSystemFlush.class.getMethod(
            "testTracingHeaderForAppendBlob")); //outputstream (appendblob)
    testClasses.put(new ITestAzureBlobFileSystemCreate(),
        ITestAzureBlobFileSystemCreate.class
            .getMethod("testDefaultCreateOverwriteFileTest")); //create
    testClasses.put(new ITestAzureBlobFilesystemAcl(),
        ITestAzureBlobFilesystemAcl.class
            .getMethod("testDefaultAclRenamedFile")); //rename
    testClasses.put(new ITestAzureBlobFileSystemDelete(),
        ITestAzureBlobFileSystemDelete.class
            .getMethod("testDeleteFirstLevelDirectory")); //delete
    testClasses.put(new ITestAzureBlobFileSystemCreate(),
        ITestAzureBlobFileSystemCreate.class
            .getMethod("testCreateNonRecursive")); //mkdirs
    testClasses.put(new ITestAzureBlobFileSystemAttributes(),
        ITestAzureBlobFileSystemAttributes.class
            .getMethod("testSetGetXAttr")); //setxattr, getxattr
    testClasses.put(new ITestAzureBlobFilesystemAcl(),
        ITestAzureBlobFilesystemAcl.class.getMethod(
            "testEnsureAclOperationWorksForRoot")); // setacl, getaclstatus,
    // setowner, setpermission, modifyaclentries,
    // removeaclentries, removedefaultacl, removeacl

    for (AbstractAbfsIntegrationTest testClass : testClasses.keySet()) {
      try {
        testClass.setup();
        testClasses.get(testClass).invoke(testClass);
        testClass.teardown();
      } catch (InvocationTargetException e) {
        if (!(e.getCause() instanceof AssumptionViolatedException)) {
          throw new IOException(testClasses.get(testClass).getName()
              + " failed tracing context validation test");
        }
      }
    }
  }

  @Test
  public void testExternalOps() throws Exception {
    //validate tracing header for access, hasPathCapability
    AzureBlobFileSystem fs = getFileSystem();

    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.HAS_PATH_CAPABILITY, false,
        0));

    // unset namespaceEnabled to call getAcl -> trigger tracing header validator
    fs.getAbfsStore().setNamespaceEnabled(Trilean.UNKNOWN);
    fs.hasPathCapability(new Path("/"), CommonPathCapabilities.FS_ACLS);

    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    Assume.assumeTrue(getConfiguration().isCheckAccessEnabled());
    Assume.assumeTrue(getAuthType() == AuthType.OAuth);

    fs.setListenerOperation(FSOperationType.ACCESS);
    fs.getAbfsStore().setNamespaceEnabled(Trilean.TRUE);
    fs.access(new Path("/"), FsAction.READ);
  }

  @Test
  public void testRetryPrimaryRequestIdWhenInitiallySuppliedEmpty() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final String fileSystemId = fs.getFileSystemId();
    final String clientCorrelationId = fs.getClientCorrelationId();
    final TracingHeaderFormat tracingHeaderFormat = TracingHeaderFormat.ALL_ID_FORMAT;
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.CREATE_FILESYSTEM, tracingHeaderFormat, new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE_FILESYSTEM, false,
        0));
    AbfsHttpOperation abfsHttpOperation = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doNothing().when(abfsHttpOperation).setRequestProperty(Mockito.anyString(), Mockito.anyString());
    tracingContext.constructHeader(abfsHttpOperation, null);
    String header = tracingContext.getHeader();
    String clientRequestIdUsed = header.split(":")[1];
    String[] clientRequestIdUsedParts = clientRequestIdUsed.split("-");
    String assertionPrimaryId = clientRequestIdUsedParts[clientRequestIdUsedParts.length - 1];

    tracingContext.setRetryCount(1);
    tracingContext.setListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE_FILESYSTEM, false,
        1));

    tracingContext.constructHeader(abfsHttpOperation, "RT");
    header = tracingContext.getHeader();
    String primaryRequestId = header.split(":")[3];

    Assertions.assertThat(primaryRequestId)
        .describedAs("PrimaryRequestId in a retried request's "
            + "tracingContext should be equal to last part of original "
            + "request's clientRequestId UUID")
        .isEqualTo(assertionPrimaryId);
  }

  @Test
  public void testRetryPrimaryRequestIdWhenInitiallySuppliedNonEmpty() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final String fileSystemId = fs.getFileSystemId();
    final String clientCorrelationId = fs.getClientCorrelationId();
    final TracingHeaderFormat tracingHeaderFormat = TracingHeaderFormat.ALL_ID_FORMAT;
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.CREATE_FILESYSTEM, tracingHeaderFormat, new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE_FILESYSTEM, false,
        0));
    tracingContext.setPrimaryRequestID();
    AbfsHttpOperation abfsHttpOperation = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doNothing().when(abfsHttpOperation).setRequestProperty(Mockito.anyString(), Mockito.anyString());
    tracingContext.constructHeader(abfsHttpOperation, null);
    String header = tracingContext.getHeader();
    String assertionPrimaryId = header.split(":")[3];

    tracingContext.setRetryCount(1);
    tracingContext.setListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE_FILESYSTEM, false,
        1));

    tracingContext.constructHeader(abfsHttpOperation, "RT");
    header = tracingContext.getHeader();
    String primaryRequestId = header.split(":")[3];

    Assertions.assertThat(primaryRequestId)
        .describedAs("PrimaryRequestId in a retried request's tracingContext "
            + "should be equal to PrimaryRequestId in the original request.")
        .isEqualTo(assertionPrimaryId);
  }
}
