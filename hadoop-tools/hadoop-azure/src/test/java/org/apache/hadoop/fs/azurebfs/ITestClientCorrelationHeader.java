package org.apache.hadoop.fs.azurebfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;

//import org.graalvm.compiler.debug.Assertions;
import org.junit.Test;
import org.assertj.core.api.Assertions;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.List;

//import static org.powermock.api.easymock.PowerMock.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public final class ITestClientCorrelationHeader extends AbstractAbfsIntegrationTest {
  AzureBlobFileSystem fs = getFileSystem();  //ensure new instance of fs is created here

  public ITestClientCorrelationHeader() throws Exception {
    super();
  }

  public void testAllOperations() throws IOException {
    fs.testTracing = true;  //store request headers

    // doing one op first. Replace with a loop over all ops or use other tests later
    fs.open(new Path("testFolder"));
    List<String> headerList = fs.headers;
    String fs_id = headerList.get(0).split(":")[2];
    String corr_id = headerList.get(0).split(":")[0];
    for (String header : headerList) {
      testHeader(header, "OF");
      Assertions.assertThat(fs_id)
              .describedAs("filesystem id should be same for requests with same filesystem")
              .isEqualTo(header.split(":")[2]);
      Assertions.assertThat(corr_id)
              .describedAs("correlation id should be same for requests using a given config")
              .isEqualTo(header.split(":")[0]);
    }
    fs.headers = null;
    fs.testTracing = false;
  }

  public void testHeader(String responseHeader, String operation) {
    boolean isContinuationOp = false;
    switch (operation) {
      case 'LS':
      case 'CR':
      case 'RD': isContinuationOp = true;
    }
    String[] headerList = responseHeader.split(":");
    // check if all IDs included
    Assertions.assertThat(headerList)
            .describedAs("header should have 7 elements").hasSize(7);

    //check that necessary ids are not empty
    Assertions.assertThat(headerList[1]).isNotEmpty();  //client request id
    Assertions.assertThat(headerList[2]).isNotEmpty();  //filesystem id

    checkClientCorrelationId();

    //check that primary req id is present when required
    if (isContinuationOp) {
      Assertions.assertThat(headerList[3]).describedAs("Continuation ops should have a primary request id")
              .isNotEmpty();
    }

  }

  void checkClientCorrelationId() {
    //check if non-empty corr-id satisfies the constraints (copy old test)
  }

  public void testPrimaryRequestID() {
    // readahead
    // liststatus
    // createoverwrite
    // rename

    //check client correlation id
  }

  public void testStreamID() {
    // create one input and one output stream. All req on a given stream should have same stream id throughout
    // placed correctly? should be in slot 4 while parsing (
  }

  public void testFilesystemID() {
    // all requests using a given filesystem instance should have same fs id
    // in slot 0
  }
/*
  @Test
//  @PrepareForTest(AbfsRestOperation.class)
  public void testRetryCount() throws IOException {
//        AbfsRestOperation op = PowerMock.spy(AbfsRestOperation.class, "executeHttpOperation");
//        PowerMockito.doThrow(new AbfsRestOperationException(new HttpException()))
//                .when(op, "executeHttpOperation", any(int),
//        any(TracingContext.class));

    AbfsClient client = mock(AbfsClient.class);

    AbfsRestOperation restop = new AbfsRestOperation(
        org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.CreatePath,
        client,
        HTTP_METHOD_PUT,
        new java.net.URL("url"),
        new java.util.ArrayList<org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader>());
    AbfsRestOperation op = spy(restop);


//    AbfsRestOperation op = spy(restop);
//    when(op.executeHttpOperation(any(int.class), any(TracingContext.class)))
//        .thenReturn(false);
//        .thenThrow(AzureBlobFileSystemException.class);

    op.execute(new TracingContext("test-corr-id", "fsid","op"));

    String path = getRelativePath(new Path("/testDir"));
    boolean isNamespaceEnabled = true;//fs.getIsNamespaceEnabled();
    String permission = null;// isNamespaceEnabled ? getOctalNotation(FsPermission.getDirDefault()) : null;
    String umask = null; //isNamespaceEnabled ? getOctalNotation(FsPermission.getUMask(fs.getConf())) : null;

//    AbfsClient client = mock(AbfsClient.class);
    when(client.createPath(any(String.class), eq(true), eq(false),
        eq(null), eq(null),
        any(boolean.class), eq(null), any(TracingContext.class))).thenReturn(op);
    AbfsRestOperation op1 = client.createPath(path, false, true,
        permission, umask, false, null,
        new TracingContext("test-corr-id", "fsid", "CR"));

  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }*/

}