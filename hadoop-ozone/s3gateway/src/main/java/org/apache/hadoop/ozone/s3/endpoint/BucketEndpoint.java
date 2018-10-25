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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.DeletedObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.Error;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket level rest endpoints.
 */
@Path("/{bucket}")
public class BucketEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BucketEndpoint.class);

  /**
   * Rest endpoint to list objects in a specific bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
   * for more details.
   */
  @GET
  public Response list(
      @PathParam("bucket") String bucketName,
      @QueryParam("delimiter") String delimiter,
      @QueryParam("encoding-type") String encodingType,
      @QueryParam("marker") String marker,
      @DefaultValue("1000") @QueryParam("max-keys") int maxKeys,
      @QueryParam("prefix") String prefix,
      @QueryParam("browser") String browser,
      @Context HttpHeaders hh) throws OS3Exception, IOException {

    if (browser != null) {
      try (InputStream browserPage = getClass()
          .getResourceAsStream("/browser.html")) {
        return Response.ok(browserPage,
            MediaType.TEXT_HTML_TYPE)
            .build();
      }
    }

    if (delimiter == null) {
      delimiter = "/";
    }
    if (prefix == null) {
      prefix = "";
    }

    OzoneBucket bucket = getBucket(bucketName);

    Iterator<? extends OzoneKey> ozoneKeyIterator = bucket.listKeys(prefix);

    ListObjectResponse response = new ListObjectResponse();
    response.setDelimiter(delimiter);
    response.setName(bucketName);
    response.setPrefix(prefix);
    response.setMarker("");
    response.setMaxKeys(1000);
    response.setEncodingType("url");
    response.setTruncated(false);

    String prevDir = null;
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey next = ozoneKeyIterator.next();
      String relativeKeyName = next.getName().substring(prefix.length());

      int depth =
          StringUtils.countMatches(relativeKeyName, delimiter);

      if (prefix.length() > 0 && !prefix.endsWith(delimiter)
          && relativeKeyName.length() > 0) {
        response.addPrefix(prefix + "/");
        break;
      }
      if (depth > 0) {
        String dirName = relativeKeyName
            .substring(0, relativeKeyName.indexOf(delimiter));
        if (!dirName.equals(prevDir)) {
          response.addPrefix(
              prefix + dirName + delimiter);
          prevDir = dirName;
        }
      } else if (relativeKeyName.endsWith(delimiter)) {
        response.addPrefix(relativeKeyName);
      } else if (relativeKeyName.length() > 0) {
        KeyMetadata keyMetadata = new KeyMetadata();
        keyMetadata.setKey(next.getName());
        keyMetadata.setSize(next.getDataSize());
        keyMetadata.setETag("" + next.getModificationTime());
        keyMetadata.setStorageClass("STANDARD");
        keyMetadata
            .setLastModified(Instant.ofEpochMilli(next.getModificationTime()));
        response.addKey(keyMetadata);
      }
    }
    response.setKeyCount(
        response.getCommonPrefixes().size() + response.getContents().size());
    return Response.ok(response).build();
  }

  @PUT
  public Response put(@PathParam("bucket") String bucketName, @Context
      HttpHeaders httpHeaders) throws IOException, OS3Exception {

    String userName = getAuthenticationHeaderParser().getAccessKeyID();

    String location = createS3Bucket(userName, bucketName);

    LOG.info("Location is {}", location);
    return Response.status(HttpStatus.SC_OK).header("Location", location)
        .build();

  }

  /**
   * Rest endpoint to check the existence of a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(@PathParam("bucket") String bucketName)
      throws OS3Exception, IOException {
    try {
      getBucket(bucketName);
    } catch (OS3Exception ex) {
      LOG.error("Exception occurred in headBucket", ex);
      //TODO: use a subclass fo OS3Exception and catch it here.
      if (ex.getCode().contains("NoSuchBucket")) {
        return Response.status(Status.BAD_REQUEST).build();
      } else {
        throw ex;
      }
    }
    return Response.ok().build();
  }

  /**
   * Rest endpoint to delete specific bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * for more details.
   */
  @DELETE
  public Response delete(@PathParam("bucket") String bucketName)
      throws IOException, OS3Exception {

    try {
      deleteS3Bucket(bucketName);
    } catch (IOException ex) {
      if (ex.getMessage().contains("BUCKET_NOT_EMPTY")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .BUCKET_NOT_EMPTY, bucketName);
        throw os3Exception;
      } else if (ex.getMessage().contains("BUCKET_NOT_FOUND")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .NO_SUCH_BUCKET, bucketName);
        throw os3Exception;
      } else {
        throw ex;
      }
    }

    return Response
        .status(HttpStatus.SC_NO_CONTENT)
        .build();

  }

  /**
   * Implement multi delete.
   * <p>
   * see: https://docs.aws.amazon
   * .com/AmazonS3/latest/API/multiobjectdeleteapi.html
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response multiDelete(@PathParam("bucket") String bucketName,
      @QueryParam("delete") String delete,
      MultiDeleteRequest request) throws OS3Exception, IOException {
    OzoneBucket bucket = getBucket(bucketName);
    MultiDeleteResponse result = new MultiDeleteResponse();
    if (request.getObjects() != null) {
      for (DeleteObject keyToDelete : request.getObjects()) {
        try {
          bucket.deleteKey(keyToDelete.getKey());

          if (!request.isQuiet()) {
            result.addDeleted(new DeletedObject(keyToDelete.getKey()));
          }
        } catch (IOException ex) {
          if (!ex.getMessage().contains("KEY_NOT_FOUND")) {
            result.addError(
                new Error(keyToDelete.getKey(), "InternalError",
                    ex.getMessage()));
          } else if (!request.isQuiet()) {
            result.addDeleted(new DeletedObject(keyToDelete.getKey()));
          }
        } catch (Exception ex) {
          result.addError(
              new Error(keyToDelete.getKey(), "InternalError",
                  ex.getMessage()));
        }
      }
    }
    ResponseBuilder response = Response.ok();
    if (!request.isQuiet() || result.getErrors().size() > 0) {
      response = response.entity(result);
    }
    return response.build();

  }
}
