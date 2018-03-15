/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import static org.apache.hadoop.ozone.web.util.ServerUtils.releaseConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Ozone Volume Class.
 */
public class OzoneVolume {
  private VolumeInfo volumeInfo;
  private Map<String, String> headerMap;
  private final OzoneRestClient client;

  /**
   * Constructor for OzoneVolume.
   */
  public OzoneVolume(OzoneRestClient client) {
    this.client = client;
    this.headerMap = new HashMap<>();
  }

  /**
   * Constructor for OzoneVolume.
   *
   * @param volInfo - volume Info.
   * @param client  Client
   */
  public OzoneVolume(VolumeInfo volInfo, OzoneRestClient client) {
    this.volumeInfo = volInfo;
    this.client = client;
  }

  /**
   * Returns a Json String of this class.
   * @return String
   * @throws IOException
   */
  public String getJsonString() throws IOException {
    return volumeInfo.toJsonString();
  }

  /**
   * sets the Volume Info.
   *
   * @param volInfoString - Volume Info String
   */
  public void setVolumeInfo(String volInfoString) throws IOException {
    this.volumeInfo = VolumeInfo.parse(volInfoString);
  }

  /**
   * @return the volume info.
   */
  public VolumeInfo getVolumeInfo() {
    return this.volumeInfo;
  }

  /**
   * Returns volume Name.
   *
   * @return Volume Name.
   */
  public String getVolumeName() {
    return this.volumeInfo.getVolumeName();
  }

  /**
   * Get created by.
   *
   * @return String
   */
  public String getCreatedby() {
    return this.volumeInfo.getCreatedBy();
  }

  /**
   * returns the Owner name.
   *
   * @return String
   */
  public String getOwnerName() {
    return this.volumeInfo.getOwner().getName();
  }

  /**
   * Returns Quota Info.
   *
   * @return Quota
   */
  public OzoneQuota getQuota() {
    return volumeInfo.getQuota();
  }

  /**
   * Returns creation time of Volume.
   *
   * @return String
   */
  public String getCreatedOn() {
    return volumeInfo.getCreatedOn();
  }

  /**
   * Returns a Http header from the Last Volume related call.
   *
   * @param headerName - Name of the header
   * @return - Header Value
   */
  public String getHeader(String headerName) {
    return headerMap.get(headerName);
  }

  /**
   * Gets the Client, this is used by Bucket and Key Classes.
   *
   * @return - Ozone Client
   */
  OzoneRestClient getClient() {
    return client;
  }

  /**
   * Create Bucket - Creates a bucket under a given volume.
   *
   * @param bucketName - Bucket Name
   * @param acls - Acls - User Acls
   * @param storageType - Storage Class
   * @param versioning - enable versioning support on a bucket.
   *
   *
   * @return - a Ozone Bucket Object
   */
  public OzoneBucket createBucket(String bucketName, String[] acls,
                                  StorageType storageType,
                                  OzoneConsts.Versioning versioning)
      throws OzoneException {

    HttpPost httpPost = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(bucketName);
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName() + "/" + bucketName).build();

      httpPost = client.getHttpPost(null, builder.toString());
      if (acls != null) {
        for (String acl : acls) {
          httpPost
              .addHeader(Header.OZONE_ACLS, Header.OZONE_ACL_ADD + " " + acl);
        }
      }

      httpPost.addHeader(Header.OZONE_STORAGE_TYPE, storageType.toString());
      httpPost.addHeader(Header.OZONE_BUCKET_VERSIONING, versioning.toString());
      executeCreateBucket(httpPost, httpClient);
      return getBucket(bucketName);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      releaseConnection(httpPost);
    }
  }

  /**
   * Create Bucket.
   *
   * @param bucketName - bucket name
   * @param acls - acls
   * @param storageType - storage class
   *
   * @throws OzoneException
   */
  public OzoneBucket createBucket(String bucketName, String[] acls,
                                  StorageType storageType)
      throws OzoneException {
    return createBucket(bucketName, acls, storageType,
        OzoneConsts.Versioning.DISABLED);
  }

  /**
   * Create Bucket.
   *
   * @param bucketName - bucket name
   * @param acls - acls
   *
   * @throws OzoneException
   */
  public OzoneBucket createBucket(String bucketName, String[] acls)
      throws OzoneException {
    return createBucket(bucketName, acls, StorageType.DEFAULT,
        OzoneConsts.Versioning.DISABLED);
  }


  /**
   * Create Bucket.
   *
   * @param bucketName - bucket name
   *
   * @throws OzoneException
   */
  public OzoneBucket createBucket(String bucketName) throws OzoneException {
    return createBucket(bucketName, null,  StorageType.DEFAULT,
        OzoneConsts.Versioning.DISABLED);
  }


  /**
   * execute a Create Bucket Request against Ozone server.
   *
   * @param httppost - httpPost
   *
   * @throws IOException
   * @throws OzoneException
   */
  private void executeCreateBucket(HttpPost httppost,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(httppost);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();
      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return;
      }

      if (entity != null) {
        throw OzoneException.parse(EntityUtils.toString(entity));
      } else {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Adds Acls to an existing bucket.
   *
   * @param bucketName - Name of the bucket
   * @param acls - Acls
   *
   * @throws OzoneException
   */
  public void addAcls(String bucketName, String[] acls) throws OzoneException {
    HttpPut putRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(bucketName);
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName() + "/" + bucketName).build();
      putRequest = client.getHttpPut(builder.toString());

      for (String acl : acls) {
        putRequest
            .addHeader(Header.OZONE_ACLS, Header.OZONE_ACL_ADD + " " + acl);
      }
      executePutBucket(putRequest, httpClient);
    } catch (URISyntaxException | IOException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      releaseConnection(putRequest);
    }
  }

  /**
   * Removes ACLs from a bucket.
   *
   * @param bucketName - Bucket Name
   * @param acls - Acls to be removed
   *
   * @throws OzoneException
   */
  public void removeAcls(String bucketName, String[] acls)
      throws OzoneException {
    HttpPut putRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(bucketName);
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName() + "/" + bucketName).build();
      putRequest = client.getHttpPut(builder.toString());

      for (String acl : acls) {
        putRequest
            .addHeader(Header.OZONE_ACLS, Header.OZONE_ACL_REMOVE + " " + acl);
      }
      executePutBucket(putRequest, httpClient);
    } catch (URISyntaxException | IOException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      releaseConnection(putRequest);
    }
  }

  /**
   * Returns information about an existing bucket.
   *
   * @param bucketName - BucketName
   *
   * @return OZoneBucket
   */
  public OzoneBucket getBucket(String bucketName) throws OzoneException {
    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(bucketName);
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName() + "/" + bucketName)
        .setParameter(Header.OZONE_INFO_QUERY_TAG,
            Header.OZONE_INFO_QUERY_BUCKET).build();
      getRequest = client.getHttpGet(builder.toString());
      return executeInfoBucket(getRequest, httpClient);

    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      releaseConnection(getRequest);
    }
  }


  /**
   * Execute the info bucket call.
   *
   * @param getRequest - httpGet Request
   * @param httpClient - Http Client
   *
   * @return OzoneBucket
   *
   * @throws IOException
   * @throws OzoneException
   */
  private OzoneBucket executeInfoBucket(HttpGet getRequest,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();
      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }
      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        OzoneBucket bucket =
            new OzoneBucket(BucketInfo.parse(EntityUtils.toString(entity)),
                this);
        return bucket;
      }
      throw OzoneException.parse(EntityUtils.toString(entity));
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Execute the put bucket call.
   *
   * @param putRequest - http put request
   * @param httpClient - Http Client
   *
   * @return OzoneBucket
   *
   * @throws IOException
   * @throws OzoneException
   */
  private void executePutBucket(HttpPut putRequest,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(putRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (errorCode == HTTP_OK) {
        return;
      }

      if (entity != null) {
        throw OzoneException.parse(EntityUtils.toString(entity));
      }

      throw new OzoneRestClientException("Unexpected null in http result");
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Gets a list of buckets on this volume.
   *
   * @return - List of buckets
   *
   * @throws OzoneException
   */
  public List<OzoneBucket> listBuckets(String resultLength,
      String previousBucket, String prefix) throws OzoneException {
    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName()).build();
      if (!Strings.isNullOrEmpty(resultLength)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_MAXKEYS, resultLength);
      }
      if (!Strings.isNullOrEmpty(previousBucket)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREVKEY, previousBucket);
      }
      if (!Strings.isNullOrEmpty(prefix)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix);
      }

      getRequest = client.getHttpGet(builder.toString());
      return executeListBuckets(getRequest, httpClient);

    } catch (IOException | URISyntaxException e) {
      throw new OzoneRestClientException(e.getMessage(), e);
    } finally {
      releaseConnection(getRequest);
    }
  }

  /**
   * executes the List Bucket Call.
   *
   * @param getRequest - http Request
   * @param httpClient - http Client
   *
   * @return List of OzoneBuckets
   *
   * @throws IOException
   * @throws OzoneException
   */
  private List<OzoneBucket> executeListBuckets(HttpGet getRequest,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    List<OzoneBucket> ozoneBucketList = new LinkedList<OzoneBucket>();
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();

      entity = response.getEntity();

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }
      if (errorCode == HTTP_OK) {
        ListBuckets bucketList =
            ListBuckets.parse(EntityUtils.toString(entity));

        for (BucketInfo info : bucketList.getBuckets()) {
          ozoneBucketList.add(new OzoneBucket(info, this));
        }
        return ozoneBucketList;

      } else {
        throw OzoneException.parse(EntityUtils.toString(entity));
      }
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Delete an empty bucket.
   *
   * @param bucketName - Name of the bucket to delete
   *
   * @throws OzoneException
   */
  public void deleteBucket(String bucketName) throws OzoneException {
    HttpDelete delRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(bucketName);
      URIBuilder builder = new URIBuilder(getClient().getEndPointURI());
      builder.setPath("/" + getVolumeName() + "/" + bucketName).build();

      delRequest = client.getHttpDelete(builder.toString());
      executeDeleteBucket(delRequest, httpClient);

    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      releaseConnection(delRequest);
    }
  }

  /**
   * Executes delete bucket call.
   *
   * @param delRequest - Delete Request
   * @param httpClient - Http Client
7   *
   * @throws IOException
   * @throws OzoneException
   */
  private void executeDeleteBucket(HttpDelete delRequest,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(delRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (errorCode == HTTP_OK) {
        return;
      }

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload.");
      }

      throw OzoneException.parse(EntityUtils.toString(entity));

    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  @VisibleForTesting
  public CloseableHttpClient newHttpClient() {
    return OzoneClientUtils.newHttpClient();
  }
}
