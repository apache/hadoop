package org.apache.hadoop.fs.azurebfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.*;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;

//import org.graalvm.compiler.debug.Assertions;
import org.junit.Test;
import org.assertj.core.api.Assertions;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import static org.powermock.api.easymock.PowerMock.*;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_CORRELATIONID;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public final class ITestClientCorrelationHeader extends AbstractAbfsIntegrationTest {
//  AzureBlobFileSystem fs = getFileSystem();  //ensure new instance of fs is created here
  private static final int HTTP_CREATED = 201;
  private static final String[] CLIENT_CORRELATIONID_LIST = {
          "valid-corr-id-123", "inval!d", ""};

  public ITestClientCorrelationHeader() throws Exception {
    super();
  }

  @Test
  public void testCreateOverwrite() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext tracingContext = new TracingContext("corr-id",
            fs.getFileSystemID(), "CR");
    AbfsOutputStream out = (AbfsOutputStream) abfsStore.createFile(
            new Path("/testFolder"), fs.getFsStatistics(),true,
            FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()),
            tracingContext);
    tracingContext.headers.clear();
    System.out.println("now second call");
    out = (AbfsOutputStream) abfsStore.createFile(
            new Path("/testFolder"), fs.getFsStatistics(),true,
            FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()),
            tracingContext);

    ArrayList<String> headers = tracingContext.getRequestHeaders();
    testHeaders(headers);
    testPrimaryRequestID(headers);
  }

  @Test
  public void testListStatus() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext tracingContext = new TracingContext("corr-id",
            fs.getFileSystemID(), "LS", "preqid");
    abfsStore.listStatus(new Path("/"), tracingContext);
    ArrayList<String> headers = tracingContext.getRequestHeaders();
    for (String header : headers) {
      System.out.println(header);
    }
    testHeaders(headers);
    testPrimaryRequestID(headers);
  }

//  @Test
//  public void testStream() throws IOException {
//  }

  public void testHeaders(ArrayList<String> headers) {
    String operation = headers.get(0).split(":")[5];
    boolean isContinuationOp = false;
    boolean isStreamOp = false;
    switch (operation) {
      case "LS":
      case "CR":
      case "RD": isContinuationOp = true;
    }
    for (String headerList : headers) {
      String[] id_list = headerList.split(":");
      System.out.println(headerList);
      // check format for parsing
      Assertions.assertThat(id_list)
              .describedAs("header should have 7 elements").hasSize(7);

      //check that necessary ids are not empty
      Assertions.assertThat(id_list[1]).isNotEmpty();  //client request id
      Assertions.assertThat(id_list[2]).isNotEmpty();  //filesystem id

      //check that primary req id is present when required
      if (isContinuationOp) {
        Assertions.assertThat(id_list[3]).describedAs("Continuation ops should have a primary request id")
                .isNotEmpty();
      }
    }

    //test values
    String fs_id = headers.get(0).split(":")[2];
    String corr_id = headers.get(0).split(":")[0];
    for (String headerList : headers) {
      String[] id_list = headerList.split(":");
      Assertions.assertThat(fs_id)
              .describedAs("filesystem id should be same for requests with same filesystem")
              .isEqualTo(id_list[2]);
      Assertions.assertThat(corr_id)
              .describedAs("correlation id should be same for requests using a given config")
              .isEqualTo(id_list[0]);
      Assertions.assertThat(Integer.parseInt(id_list[6]))
              .describedAs("Max retries allowed = 30")
              .isLessThan(30);
    }
  }

  public void testPrimaryRequestID(ArrayList<String> headerList) {
    // readahead
    // liststatus
    // createoverwrite
    // rename
    String preq = headerList.get(0).split(":")[3];
    for (String header : headerList) {
      Assertions.assertThat (header.split(":")[3])
              .describedAs("preq id should be same for given set of requests")
              .isEqualTo(preq);
    }
  }

  public void checkStreamID(String streamID, ArrayList<String> headers) {
    for (String header : headers) {
      Assertions.assertThat(header.split(":")[4])
              .describedAs("stream id should be same for a given stream")
              .isEqualTo(streamID);
    }
  }

  @Test
  public void testClientCorrelationId() throws IOException {
    checkRequest(CLIENT_CORRELATIONID_LIST[0], true);
    checkRequest(CLIENT_CORRELATIONID_LIST[1], false);
    checkRequest(CLIENT_CORRELATIONID_LIST[2], false);
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }

  public void checkRequest(String clientCorrelationId, boolean includeInHeader)
          throws IOException {
    org.apache.hadoop.conf.Configuration config = new Configuration(this.getRawConfiguration());
    config.set(FS_AZURE_CLIENT_CORRELATIONID, clientCorrelationId);

    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
            .newInstance(this.getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsClient();
//    TracingContext
    String path = getRelativePath(new Path("/testDir"));
    boolean isNamespaceEnabled = fs.getIsNamespaceEnabled();
    String permission = isNamespaceEnabled ? getOctalNotation(FsPermission.getDirDefault()) : null;
    String umask = isNamespaceEnabled ? getOctalNotation(FsPermission.getUMask(fs.getConf())) : null;
    AbfsRestOperation op = client.createPath(path, false, true,
            permission, umask, false, null,
            new TracingContext(clientCorrelationId,
                    fs.getFileSystemID(), "CR"));

    int responseCode = op.getResult().getStatusCode();
    Assertions.assertThat(responseCode).describedAs("Status code").isEqualTo(HTTP_CREATED);

//    op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
    Assertions.assertThat(responseCode).describedAs("Status code").isEqualTo(HTTP_CREATED);

    String requestHeader = op.getResult().getRequestHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
    List<String> clientRequestIds = java.util.Arrays.asList(
            requestHeader.replace("[","")
                    .replace("]", "")
                    .split(":"));
    if (includeInHeader) {
      Assertions.assertThat(clientRequestIds)
              .describedAs("There should be 7 items in the header when valid clientCorrelationId is set")
              .hasSize(7);
      Assertions.assertThat(clientRequestIds)
              .describedAs("clientCorrelation should be included in the header")
              .contains(clientCorrelationId);
    } else if (clientCorrelationId.length() > 0){
      Assertions.assertThat(clientRequestIds)
              .describedAs("There should be only 6 item in the header when invalid clientCorrelationId is set")
              .hasSize(6);
      Assertions.assertThat(clientRequestIds)
              .describedAs("Invalid or empty correlationId value should not be included in header")
              .doesNotContain(clientCorrelationId);
    }
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