package org.apache.hadoop.fs.azurebfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsOperationConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingContextFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class TestTracingContext extends AbstractAbfsIntegrationTest {
  private static final String[] CLIENT_CORRELATIONID_LIST = {
      "valid-corr-id-123", "inval!d", ""};
  private static final int HTTP_CREATED = 201;
  private final String EMPTY_STRING = "";
  String GUID_PATTERN = "[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}";
  String prevClientRequestID = "";

  public TestTracingContext() throws Exception {
    super();
  }

  @Test
  public void testClientCorrelationID() throws IOException {
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[0], true);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[1], false);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[2], false);
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  public void checkCorrelationConfigValidation(String clientCorrelationId,
      boolean includeInHeader) throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fs.getFileSystemID(), AbfsOperationConstants.TESTOP,
        TracingContextFormat.ALL_ID_FORMAT,null);
    String correlationID = tracingContext.toString().split(":")[0];
    if (includeInHeader) {
      Assertions.assertThat(correlationID)
          .describedAs("Correlation ID should match config when valid")
          .isEqualTo(clientCorrelationId);
    } else {
      Assertions.assertThat(correlationID)
          .describedAs("Invalid ID should be replaced with empty string")
          .isEqualTo(EMPTY_STRING);
    }

    //request should not fail for invalid clientCorrelationID
    fs.getAbfsStore().setNamespaceEnabled(Trilean.getTrilean(true));
    AbfsRestOperation op = fs.getAbfsStore().getClient().createPath("/testDir",
        false, true, getOctalNotation(FsPermission.getDefault()),
        getOctalNotation(FsPermission.getUMask(getRawConfiguration())),
        false, null, tracingContext);

    int statusCode = op.getResult().getStatusCode();
    Assertions.assertThat(statusCode).describedAs("Request should not fail")
        .isEqualTo(HTTP_CREATED);

    String requestHeader = op.getResult().getRequestHeader(
        HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID)
        .replace("[", "").replace("]", "");
    Assertions.assertThat(requestHeader)
        .describedAs("Client Request Header should match TracingContext")
        .isEqualTo(tracingContext.toString());

    // use fn below or pass listener to run all TracingHeaderValidator checks
    checkRequiredIDs(requestHeader);
  }

  private void checkRequiredIDs(String requestHeader) {
    String[] id_list = requestHeader.split(":");

    Assertions.assertThat(id_list[1])
        .describedAs("client-req-id should be a guid")
        .matches(GUID_PATTERN);
    Assertions.assertThat(id_list[2])
        .describedAs("filesystem-id should not be empty")
        .isNotEmpty();
    Assertions.assertThat(id_list[1])
        .describedAs("client-request-id should be unique")
        .isNotEqualTo(prevClientRequestID);
  }

  @Ignore
  @Test
  //call test methods from the respective test classes
  //can be ignored when running all tests as these get covered
  public void runCorrelationTestForAllMethods() throws Exception {
    //map to avoid creating new instance and calling setup() for each test
    Map<AbstractAbfsIntegrationTest, Method> testClasses = new HashMap<>();

//    testClasses.put(new ITestAzureBlobFileSystemListStatus(), //liststatus
//        ITestAzureBlobFileSystemListStatus.class.getMethod("testListPath"));
    testClasses.put(new ITestAbfsReadWriteAndSeek(32), //open, read, write
    ITestAbfsReadWriteAndSeek.class.getMethod("testReadAheadRequestID"));
    testClasses.put(new ITestAbfsReadWriteAndSeek(32), //read
        ITestAbfsReadWriteAndSeek.class.getMethod("testReadAndWriteWithDifferentBufferSizesAndSeek"));
    testClasses.put(new ITestAzureBlobFileSystemAppend(), //append
        ITestAzureBlobFileSystemAppend.class.getMethod("testTracingForAppend"));
    testClasses.put(new ITestAzureBlobFileSystemCreate(),
        ITestAzureBlobFileSystemCreate.class.getMethod(
            "testDefaultCreateOverwriteFileTest")); //create
    testClasses.put(new ITestAzureBlobFilesystemAcl(),
        ITestAzureBlobFilesystemAcl.class.getMethod(
            "testDefaultAclRenamedFile")); //rename
    testClasses.put(new ITestAzureBlobFileSystemDelete(),
        ITestAzureBlobFileSystemDelete.class.getMethod(
            "testDeleteFirstLevelDirectory")); //delete
    testClasses.put(new ITestAzureBlobFileSystemCreate(),
        ITestAzureBlobFileSystemCreate.class.getMethod(
            "testCreateNonRecursive")); //mkdirs
    testClasses.put(new ITestAzureBlobFileSystemAttributes(),
        ITestAzureBlobFileSystemAttributes.class.getMethod(
            "testSetGetXAttr")); //setxattr, getxattr
    testClasses.put(new ITestAzureBlobFilesystemAcl(),
        ITestAzureBlobFilesystemAcl.class.getMethod(
            "testEnsureAclOperationWorksForRoot")); // setacl, getaclstatus,
    // setowner, setpermission, modifyaclentries,
    // removeaclentries, removedefaultacl, removeacl

    for (AbstractAbfsIntegrationTest testClass : testClasses.keySet()) {
      System.out.println(testClass.methodName.getMethodName());
      testClass.setup();
      testClasses.get(testClass).invoke(testClass);
      testClass.teardown();
    }
    testExternalOps();
  }

  @Test
  //rename this test
  public void testExternalOps() throws Exception {
    //validate tracing header for access, hasPathCapability,
    // getIsNamespaceEnabled
    AzureBlobFileSystem fs = getFileSystem();
    fs.registerListener(new TracingHeaderValidator(fs.getAbfsStore()
        .getAbfsConfiguration().getClientCorrelationID(), fs.getFileSystemID(),
        AbfsOperationConstants.ACCESS, false, 0));
    fs.access(new Path("/"), FsAction.READ);

    fs.setListenerOperation(AbfsOperationConstants.PATH);
    //unset namespaceEnabled config to call getAcl -> test tracing header
    fs.getAbfsStore().setNamespaceEnabled(Trilean.UNKNOWN);
    fs.hasPathCapability(new Path("/"), CommonPathCapabilities.FS_ACLS);

    ITestAzureBlobFileSystemAppend test2 = new ITestAzureBlobFileSystemAppend();
    test2.setup();
    test2.testTracingForAppend();
  }
}
