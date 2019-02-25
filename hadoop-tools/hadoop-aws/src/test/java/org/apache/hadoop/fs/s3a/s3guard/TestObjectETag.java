package org.apache.hadoop.fs.s3a.s3guard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3AMockTest;
import org.apache.hadoop.fs.s3a.Constants;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Tests to ensure eTag is captured on S3 PUT and used on GET.
 */
public class TestObjectETag extends AbstractS3AMockTest {

  /**
   * Tests a file uploaded with a single PUT to ensure eTag is captured and used
   * on file read.
   */
  @Test
  public void testCreateAndReadFileSinglePart() throws Exception {
    Path path = new Path("s3a://mock-bucket/file");
    String content = "content";

    PutObjectResult putObjectResult = new PutObjectResult();
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(content.length());
    putObjectResult.setMetadata(objectMetadata);
    String eTag = "abc";
    putObjectResult.setETag(eTag);

    when(s3.getObjectMetadata(any(GetObjectMetadataRequest.class)))
        .thenThrow(NOT_FOUND);
    when(s3.putObject(argThat(correctPutObjectRequest("file"))))
        .thenReturn(putObjectResult);
    ListObjectsV2Result emptyListing = new ListObjectsV2Result();
    when(s3.listObjectsV2(argThat(correctListObjectsRequest("file/"))))
        .thenReturn(emptyListing);

    FSDataOutputStream outputStream = fs.create(path);
    outputStream.writeChars(content);
    outputStream.close();

    // make sure the eTag was put into the metadataStore
    MetadataStore metadataStore = fs.getMetadataStore();
    PathMetadata pathMetadata = metadataStore.get(path);
    assertNotNull(pathMetadata);
    String storedETag = pathMetadata.getFileStatus().getETag();
    assertEquals(eTag, storedETag);

    // Ensure underlying S3 getObject call uses the stored eTag when reading
    // data back.  If it doesn't, the read won't work and the assert will
    // fail.
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(content.getBytes()));
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setHeader(Headers.ETAG, eTag);
    s3Object.setObjectMetadata(metadata);
    when(s3.getObject(argThat(correctGetObjectRequest("file", eTag))))
        .thenReturn(s3Object);
    FSDataInputStream inputStream = fs.open(path);
    String readContent = IOUtils.toString(inputStream);
    assertEquals(content, readContent);
  }

  /**
   * Tests a file uploaded with multi-part upload to ensure eTag is captured
   * and used on file read.
   */
  @Test
  public void testCreateAndReadFileMultiPart() throws Exception {
    Path path = new Path("s3a://mock-bucket/file");
    byte[] content = new byte[Constants.MULTIPART_MIN_SIZE + 1];

    CompleteMultipartUploadResult uploadResult =
        new CompleteMultipartUploadResult();
    String eTag = "abc";
    uploadResult.setETag(eTag);

    when(s3.getObjectMetadata(any(GetObjectMetadataRequest.class)))
        .thenThrow(NOT_FOUND);

    InitiateMultipartUploadResult initiateMultipartUploadResult =
        new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("uploadId");
    when(s3.initiateMultipartUpload(
        argThat(correctInitiateMultipartUploadRequest("file"))))
        .thenReturn(initiateMultipartUploadResult);

    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setETag("partETag");
    when(s3.uploadPart(argThat(correctUploadPartRequest("file"))))
        .thenReturn(uploadPartResult);

    CompleteMultipartUploadResult multipartUploadResult =
        new CompleteMultipartUploadResult();
    multipartUploadResult.setETag(eTag);
    when(s3.completeMultipartUpload(
        argThat(correctMultipartUploadRequest("file"))))
        .thenReturn(multipartUploadResult);

    ListObjectsV2Result emptyListing = new ListObjectsV2Result();
    when(s3.listObjectsV2(argThat(correctListObjectsRequest("file/"))))
        .thenReturn(emptyListing);

    FSDataOutputStream outputStream = fs.create(path);
    outputStream.write(content);
    outputStream.close();

    // make sure the eTag was put into the metadataStore
    MetadataStore metadataStore = fs.getMetadataStore();
    PathMetadata pathMetadata = metadataStore.get(path);
    assertNotNull(pathMetadata);
    String storedETag = pathMetadata.getFileStatus().getETag();
    assertEquals(eTag, storedETag);

    // Ensure underlying S3 getObject call uses the stored eTag when reading
    // data back.  If it doesn't, the read won't work and the assert will
    // fail.
    S3Object s3Object = new S3Object();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setHeader(Headers.ETAG, eTag);
    s3Object.setObjectMetadata(metadata);
    s3Object.setObjectContent(new ByteArrayInputStream(content));
    when(s3.getObject(argThat(correctGetObjectRequest("file", eTag))))
        .thenReturn(s3Object);
    FSDataInputStream inputStream = fs.open(path);
    byte[] readContent = IOUtils.toByteArray(inputStream);
    assertArrayEquals(content, readContent);
  }

  private Matcher<UploadPartRequest> correctUploadPartRequest(
      final String key) {
    return new BaseMatcher<UploadPartRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof UploadPartRequest) {
          UploadPartRequest request = (UploadPartRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<InitiateMultipartUploadRequest>
      correctInitiateMultipartUploadRequest(final String key) {
    return new BaseMatcher<InitiateMultipartUploadRequest>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }

      @Override
      public boolean matches(Object item) {
        if (item instanceof InitiateMultipartUploadRequest) {
          InitiateMultipartUploadRequest request =
              (InitiateMultipartUploadRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }
    };
  }

  private Matcher<CompleteMultipartUploadRequest>
      correctMultipartUploadRequest(final String key) {
    return new BaseMatcher<CompleteMultipartUploadRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof CompleteMultipartUploadRequest) {
          CompleteMultipartUploadRequest request =
              (CompleteMultipartUploadRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<ListObjectsV2Request> correctListObjectsRequest(
      final String key) {
    return new BaseMatcher<ListObjectsV2Request>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof ListObjectsV2Request) {
          ListObjectsV2Request listObjectsRequest =
              (ListObjectsV2Request) item;
          return listObjectsRequest.getPrefix().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<PutObjectRequest> correctPutObjectRequest(
      final String key) {
    return new BaseMatcher<PutObjectRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof PutObjectRequest) {
          PutObjectRequest putObjectRequest = (PutObjectRequest) item;
          return putObjectRequest.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<GetObjectRequest> correctGetObjectRequest(final String key,
      final String eTag) {
    return new BaseMatcher<GetObjectRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof GetObjectRequest) {
          GetObjectRequest getObjectRequest = (GetObjectRequest) item;
          return getObjectRequest.getKey().equals(key)
              && getObjectRequest.getMatchingETagConstraints()
              .contains(eTag);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key and eTag matches");
      }
    };
  }
}
