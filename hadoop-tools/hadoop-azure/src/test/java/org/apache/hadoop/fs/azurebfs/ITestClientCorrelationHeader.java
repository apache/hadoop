package org.apache.hadoop.fs.azurebfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TrackingContext;
import org.apache.http.HttpException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import org.junit.Test;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
//import static org.powermock.api.easymock.PowerMock.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public final class ITestClientCorrelationHeader extends AbstractAbfsIntegrationTest {
  AzureBlobFileSystem fs = getFileSystem();

  public ITestClientCorrelationHeader() throws Exception {
    super();
  }

  public void testPrimaryRequestID() {
    // readahead
    // liststatus
    // createoverwrite
    // rename

    // check if placed in the correct slot and matches client req id
    // how to verify for readahead - concurrent requests?

    //check client correlation id - response vs request header?
  }

  public void testStreamID() {
    // create one input and one output stream. All req on a given stream should have same stream id throughout
    // placed correctly? should be in slot 4 while parsing (
  }

  public void testFilesystemID() {
    // all requests using a given filesystem instance should have same fs id
    // in slot 0
  }

  @Test
//  @PrepareForTest(AbfsRestOperation.class)
  public void testRetryCount() throws IOException {
//        AbfsRestOperation op = PowerMock.spy(AbfsRestOperation.class, "executeHttpOperation");
//        PowerMockito.doThrow(new AbfsRestOperationException(new HttpException()))
//                .when(op, "executeHttpOperation", any(int),
//        any(TrackingContext.class));

    AbfsClient client = mock(AbfsClient.class);

    AbfsRestOperation restop = new AbfsRestOperation(
        org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.CreatePath,
        client,
        HTTP_METHOD_PUT,
        new java.net.URL("url"),
        new java.util.ArrayList<org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader>());
    AbfsRestOperation op = spy(restop);


//    AbfsRestOperation op = spy(restop);
    when(op.executeHttpOperation(any(int.class), any(TrackingContext.class)))
        .thenReturn(false);
//        .thenThrow(AzureBlobFileSystemException.class);

    op.execute(new org.apache.hadoop.fs.azurebfs.utils.TrackingContext("fsid","op"));

    String path = getRelativePath(new Path("/testDir"));
    boolean isNamespaceEnabled = true;//fs.getIsNamespaceEnabled();
    String permission = null;// isNamespaceEnabled ? getOctalNotation(FsPermission.getDirDefault()) : null;
    String umask = null; //isNamespaceEnabled ? getOctalNotation(FsPermission.getUMask(fs.getConf())) : null;

//    AbfsClient client = mock(AbfsClient.class);
    when(client.createPath(any(String.class), eq(true), eq(false),
        eq(null), eq(null),
        any(boolean.class), eq(null), any(TrackingContext.class))).thenReturn(op);
    AbfsRestOperation op1 = client.createPath(path, false, true,
        permission, umask, false, null,
        new TrackingContext("fsid", "CR"));

  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }

}