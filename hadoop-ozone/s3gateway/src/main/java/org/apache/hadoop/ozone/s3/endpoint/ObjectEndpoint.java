/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.io.S3WrapperInputStream;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.ozone.s3.util.S3utils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import static javax.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static javax.ws.rs.core.HttpHeaders.LAST_MODIFIED;
import org.apache.commons.io.IOUtils;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCEPT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CONTENT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER_SUPPORTED_UNIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key level rest endpoints.
 */
@Path("/{bucket}/{path:.+}")
public class ObjectEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpoint.class);

  @Context
  private HttpHeaders headers;

  private List<String> customizableGetHeaders = new ArrayList<>();

  public ObjectEndpoint() {
    customizableGetHeaders.add("Content-Type");
    customizableGetHeaders.add("Content-Language");
    customizableGetHeaders.add("Expires");
    customizableGetHeaders.add("Cache-Control");
    customizableGetHeaders.add("Content-Disposition");
    customizableGetHeaders.add("Content-Encoding");
  }

  /**
   * Rest endpoint to upload object to a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html for
   * more details.
   */
  @PUT
  public Response put(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @HeaderParam("Content-Length") long length,
      InputStream body) throws IOException, OS3Exception {

    OzoneOutputStream output = null;
    try {
      String copyHeader = headers.getHeaderString(COPY_SOURCE_HEADER);
      String storageType = headers.getHeaderString(STORAGE_CLASS_HEADER);

      ReplicationType replicationType;
      ReplicationFactor replicationFactor;
      boolean storageTypeDefault;
      if (storageType == null || storageType.equals("")) {
        replicationType = S3StorageType.getDefault().getType();
        replicationFactor = S3StorageType.getDefault().getFactor();
        storageTypeDefault = true;
      } else {
        try {
          replicationType = S3StorageType.valueOf(storageType).getType();
          replicationFactor = S3StorageType.valueOf(storageType).getFactor();
        } catch (IllegalArgumentException ex) {
          throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT,
              storageType);
        }
        storageTypeDefault = false;
      }

      if (copyHeader != null) {
        //Copy object, as copy source available.
        CopyObjectResponse copyObjectResponse = copyObject(
            copyHeader, bucketName, keyPath, replicationType,
            replicationFactor, storageTypeDefault);
        return Response.status(Status.OK).entity(copyObjectResponse).header(
            "Connection", "close").build();
      }

      // Normal put object
      OzoneBucket bucket = getBucket(bucketName);

      output = bucket.createKey(keyPath, length, replicationType,
          replicationFactor);

      if ("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
          .equals(headers.getHeaderString("x-amz-content-sha256"))) {
        body = new SignedChunksInputStream(body);
      }

      IOUtils.copy(body, output);

      return Response.ok().status(HttpStatus.SC_OK)
          .build();
    } catch (IOException ex) {
      LOG.error("Exception occurred in PutObject", ex);
      throw ex;
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

  /**
   * Rest endpoint to download object from a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html for
   * more details.
   */
  @GET
  public Response get(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      InputStream body) throws IOException, OS3Exception {
    try {
      OzoneBucket bucket = getBucket(bucketName);

      OzoneKeyDetails keyDetails = bucket.getKey(keyPath);

      long length = keyDetails.getDataSize();

      LOG.debug("Data length of the key {} is {}", keyPath, length);

      String rangeHeaderVal = headers.getHeaderString(RANGE_HEADER);
      RangeHeader rangeHeader = null;

      LOG.debug("range Header provided value is {}", rangeHeaderVal);

      if (rangeHeaderVal != null) {
        rangeHeader = S3utils.parseRangeHeader(rangeHeaderVal,
            length);
        LOG.debug("range Header provided value is {}", rangeHeader);
        if (rangeHeader.isInValidRange()) {
          OS3Exception exception = S3ErrorTable.newError(S3ErrorTable
              .INVALID_RANGE, rangeHeaderVal);
          throw exception;
        }
      }
      ResponseBuilder responseBuilder;

      if (rangeHeaderVal == null || rangeHeader.isReadFull()) {
        StreamingOutput output = dest -> {
          try (OzoneInputStream key = bucket.readKey(keyPath)) {
            IOUtils.copy(key, dest);
          }
        };
        responseBuilder = Response
            .ok(output)
            .header(CONTENT_LENGTH, keyDetails.getDataSize());

      } else {
        LOG.debug("range Header provided value is {}", rangeHeader);
        OzoneInputStream key = bucket.readKey(keyPath);

        long startOffset = rangeHeader.getStartOffset();
        long endOffset = rangeHeader.getEndOffset();
        long copyLength;
        if (startOffset == endOffset) {
          // if range header is given as bytes=0-0, then we should return 1
          // byte from start offset
          copyLength = 1;
        } else {
          copyLength = rangeHeader.getEndOffset() - rangeHeader
              .getStartOffset() + 1;
        }
        StreamingOutput output = dest -> {
          try (S3WrapperInputStream s3WrapperInputStream =
              new S3WrapperInputStream(
                  key.getInputStream())) {
            IOUtils.copyLarge(s3WrapperInputStream, dest, startOffset,
                copyLength);
          }
        };
        responseBuilder = Response
            .ok(output)
            .header(CONTENT_LENGTH, copyLength);

        String contentRangeVal = RANGE_HEADER_SUPPORTED_UNIT + " " +
            rangeHeader.getStartOffset() + "-" + rangeHeader.getEndOffset() +
            "/" + length;

        responseBuilder.header(CONTENT_RANGE_HEADER, contentRangeVal);
      }
      responseBuilder.header(ACCEPT_RANGE_HEADER,
          RANGE_HEADER_SUPPORTED_UNIT);
      for (String responseHeader : customizableGetHeaders) {
        String headerValue = headers.getHeaderString(responseHeader);
        if (headerValue != null) {
          responseBuilder.header(responseHeader, headerValue);
        }
      }
      addLastModifiedDate(responseBuilder, keyDetails);
      return responseBuilder.build();
    } catch (IOException ex) {
      if (ex.getMessage().contains("NOT_FOUND")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .NO_SUCH_KEY, keyPath);
        throw os3Exception;
      } else {
        throw ex;
      }
    }
  }

  private void addLastModifiedDate(
      ResponseBuilder responseBuilder, OzoneKeyDetails key) {

    ZonedDateTime lastModificationTime =
        Instant.ofEpochMilli(key.getModificationTime())
            .atZone(ZoneId.of("GMT"));

    responseBuilder
        .header(LAST_MODIFIED,
            RFC1123Util.FORMAT.format(lastModificationTime));
  }

  /**
   * Rest endpoint to check existence of an object in a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath) throws Exception {
    OzoneKeyDetails key;

    try {
      key = getBucket(bucketName).getKey(keyPath);
      // TODO: return the specified range bytes of this object.
    } catch (IOException ex) {
      LOG.error("Exception occurred in HeadObject", ex);
      if (ex.getMessage().contains("KEY_NOT_FOUND")) {
        // Just return 404 with no content
        return Response.status(Status.NOT_FOUND).build();
      } else {
        throw ex;
      }
    }

    ResponseBuilder response = Response.ok().status(HttpStatus.SC_OK)
        .header("ETag", "" + key.getModificationTime())
        .header("Content-Length", key.getDataSize())
        .header("Content-Type", "binary/octet-stream");
    addLastModifiedDate(response, key);
    return response
        .build();
  }

  /**
   * Delete a specific object from a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
   * for more details.
   */
  @DELETE
  public Response delete(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath) throws IOException, OS3Exception {

    try {
      OzoneBucket bucket = getBucket(bucketName);
      bucket.getKey(keyPath);
      bucket.deleteKey(keyPath);
    } catch (IOException ex) {
      if (ex.getMessage().contains("BUCKET_NOT_FOUND")) {
        throw S3ErrorTable.newError(S3ErrorTable
            .NO_SUCH_BUCKET, bucketName);
      } else if (!ex.getMessage().contains("NOT_FOUND")) {
        throw ex;
      }
      //NOT_FOUND is not a problem, AWS doesn't throw exception for missing
      // keys. Just return 204.
    }
    return Response
        .status(Status.NO_CONTENT)
        .build();

  }

  @VisibleForTesting
  public void setHeaders(HttpHeaders headers) {
    this.headers = headers;
  }

  private CopyObjectResponse copyObject(String copyHeader,
                                        String destBucket,
                                        String destkey,
                                        ReplicationType replicationType,
                                        ReplicationFactor replicationFactor,
                                        boolean storageTypeDefault)
      throws OS3Exception, IOException {

    if (copyHeader.startsWith("/")) {
      copyHeader = copyHeader.substring(1);
    }
    int pos = copyHeader.indexOf("/");
    if (pos == -1) {
      OS3Exception ex = S3ErrorTable.newError(S3ErrorTable
          .INVALID_ARGUMENT, copyHeader);
      ex.setErrorMessage("Copy Source must mention the source bucket and " +
          "key: sourcebucket/sourcekey");
      throw ex;
    }
    String sourceBucket = copyHeader.substring(0, pos);
    String sourceKey = copyHeader.substring(pos + 1);

    OzoneInputStream sourceInputStream = null;
    OzoneOutputStream destOutputStream = null;
    boolean closed = false;
    try {
      // Checking whether we trying to copying to it self.

      if (sourceBucket.equals(destBucket)) {
        if (sourceKey.equals(destkey)) {
          // When copying to same storage type when storage type is provided,
          // we should not throw exception, as aws cli checks if any of the
          // options like storage type are provided or not when source and
          // dest are given same
          if (storageTypeDefault) {
            OS3Exception ex = S3ErrorTable.newError(S3ErrorTable
                .INVALID_REQUEST, copyHeader);
            ex.setErrorMessage("This copy request is illegal because it is " +
                "trying to copy an object to it self itself without changing " +
                "the object's metadata, storage class, website redirect " +
                "location or encryption attributes.");
            throw ex;
          } else {
            // TODO: Actually here we should change storage type, as ozone
            // still does not support this just returning dummy response
            // for now
            CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
            copyObjectResponse.setETag(OzoneUtils.getRequestID());
            copyObjectResponse.setLastModified(Instant.ofEpochMilli(
                Time.now()));
            return copyObjectResponse;
          }
        }
      }


      OzoneBucket sourceOzoneBucket = getBucket(sourceBucket);
      OzoneBucket destOzoneBucket = getBucket(destBucket);

      OzoneKeyDetails sourceKeyDetails = sourceOzoneBucket.getKey(sourceKey);
      long sourceKeyLen = sourceKeyDetails.getDataSize();

      sourceInputStream = sourceOzoneBucket.readKey(sourceKey);

      destOutputStream = destOzoneBucket.createKey(destkey, sourceKeyLen,
          replicationType, replicationFactor);

      IOUtils.copy(sourceInputStream, destOutputStream);

      // Closing here, as if we don't call close this key will not commit in
      // OM, and getKey fails.
      sourceInputStream.close();
      destOutputStream.close();
      closed = true;

      OzoneKeyDetails destKeyDetails = destOzoneBucket.getKey(destkey);

      CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
      copyObjectResponse.setETag(OzoneUtils.getRequestID());
      copyObjectResponse.setLastModified(Instant.ofEpochMilli(destKeyDetails
          .getModificationTime()));
      return copyObjectResponse;
    } catch (IOException ex) {
      if (ex.getMessage().contains("KEY_NOT_FOUND")) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_KEY, sourceKey);
      }
      LOG.error("Exception occurred in PutObject", ex);
      throw ex;
    } finally {
      if (!closed) {
        if (sourceInputStream != null) {
          sourceInputStream.close();
        }
        if (destOutputStream != null) {
          destOutputStream.close();
        }
      }
    }
  }
}
