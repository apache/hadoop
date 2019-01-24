package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;


import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * This class tests abort multipart upload request.
 */
public class TestAbortMultipartUpload {


  @Test
  public void testAbortMultipartUpload() throws Exception {

    String bucket = "s3bucket";
    String key = "key1";
    OzoneClientStub client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("ozone", bucket);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint rest = new ObjectEndpoint();
    rest.setHeaders(headers);
    rest.setClient(client);

    Response response = rest.multipartUpload(bucket, key, "", "", null);

    assertEquals(response.getStatus(), 200);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();


    // Abort multipart upload
    response = rest.delete(bucket, key, uploadID);

    assertEquals(204, response.getStatus());

    // test with unknown upload Id.
    try {
      rest.delete(bucket, key, "random");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getErrorMessage(),
          ex.getErrorMessage());
    }

  }
}
