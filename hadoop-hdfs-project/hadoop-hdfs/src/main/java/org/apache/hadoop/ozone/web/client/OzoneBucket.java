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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.web.utils.OzoneUtils.ENCODING;
import static org.apache.hadoop.ozone.web.utils.OzoneUtils.ENCODING_NAME;

/**
 * A Bucket class the represents an Ozone Bucket.
 */
public class OzoneBucket {
  static final Logger LOG = LoggerFactory.getLogger(OzoneBucket.class);

  private BucketInfo bucketInfo;
  private OzoneVolume volume;

  /**
   * Constructor for bucket.
   *
   * @param info   - BucketInfo
   * @param volume - OzoneVolume Object that contains this bucket
   */
  public OzoneBucket(BucketInfo info, OzoneVolume volume) {
    this.bucketInfo = info;
    this.volume = volume;
  }

  /**
   * Gets bucket Info.
   *
   * @return BucketInfo
   */
  public BucketInfo getBucketInfo() {
    return bucketInfo;
  }

  /**
   * Sets Bucket Info.
   *
   * @param bucketInfo BucketInfo
   */
  public void setBucketInfo(BucketInfo bucketInfo) {
    this.bucketInfo = bucketInfo;
  }

  /**
   * Returns the parent volume class.
   *
   * @return - OzoneVolume
   */
  OzoneVolume getVolume() {
    return volume;
  }

  /**
   * Returns bucket name.
   *
   * @return Bucket Name
   */
  public String getBucketName() {
    return bucketInfo.getBucketName();
  }

  /**
   * Returns the Acls on the bucket.
   *
   * @return - Acls
   */
  public List<OzoneAcl> getAcls() {
    return bucketInfo.getAcls();
  }

  /**
   * Return versioning info on the bucket - Enabled or disabled.
   *
   * @return - Version Enum
   */
  public OzoneConsts.Versioning getVersioning() {
    return bucketInfo.getVersioning();
  }

  /**
   * Gets the Storage class for the bucket.
   *
   * @return Storage Class Enum
   */
  public StorageType getStorageType() {
    return bucketInfo.getStorageType();
  }

  /**
   * Gets the creation time of the bucket.
   *
   * @return String
   */
  public String getCreatedOn() {
    return bucketInfo.getCreatedOn();
  }

  /**
   * Puts an Object in Ozone bucket.
   *
   * @param keyName - Name of the key
   * @param data    - Data that you want to put
   * @throws OzoneException
   */
  public void putKey(String keyName, String data) throws OzoneException {
    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException("Invalid key Name.");
    }

    if (data == null) {
      throw new OzoneRestClientException("Invalid data.");
    }

    HttpPut putRequest = null;
    InputStream is = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      putRequest = getVolume().getClient().getHttpPut(builder.toString());

      is = new ByteArrayInputStream(data.getBytes(ENCODING));
      putRequest.setEntity(new InputStreamEntity(is, data.length()));
      is.mark(data.length());
      try {
        putRequest.setHeader(Header.CONTENT_MD5, DigestUtils.md5Hex(is));
      } finally {
        is.reset();
      }
      executePutKey(putRequest, httpClient);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      IOUtils.closeStream(is);
      OzoneClientUtils.releaseConnection(putRequest);
    }
  }

  /**
   * Puts an Object in Ozone Bucket.
   *
   * @param dataFile - File from which you want the data to be put. Key Name
   *                 will same as the file name, devoid of any path.
   * @throws OzoneException
   */
  public void putKey(File dataFile) throws OzoneException {
    if (dataFile == null) {
      throw new OzoneRestClientException("Invalid file object.");
    }
    String keyName = dataFile.getName();
    putKey(keyName, dataFile);
  }

  /**
   * Puts a Key in Ozone Bucket.
   *
   * @param keyName - Name of the Key
   * @param file    - Stream that gets read to be put into Ozone.
   * @throws OzoneException
   */
  public void putKey(String keyName, File file)
      throws OzoneException {

    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    if (file == null) {
      throw new OzoneRestClientException("Invalid data stream");
    }

    HttpPut putRequest = null;
    FileInputStream fis = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      putRequest = getVolume().getClient().getHttpPut(builder.toString());

      FileEntity fileEntity = new FileEntity(file, ContentType
          .APPLICATION_OCTET_STREAM);
      putRequest.setEntity(fileEntity);

      fis = new FileInputStream(file);
      putRequest.setHeader(Header.CONTENT_MD5, DigestUtils.md5Hex(fis));
      executePutKey(putRequest, httpClient);

    } catch (IOException | URISyntaxException ex) {
      final OzoneRestClientException orce = new OzoneRestClientException(
          "Failed to putKey: keyName=" + keyName + ", file=" + file);
      orce.initCause(ex);
      LOG.trace("", orce);
      throw orce;
    } finally {
      IOUtils.closeStream(fis);
      OzoneClientUtils.releaseConnection(putRequest);
    }
  }

  /**
   * executePutKey executes the Put request against the Ozone Server.
   *
   * @param putRequest - Http Put Request
   * @param httpClient - httpClient
   * @throws OzoneException
   * @throws IOException
   */
  public static void executePutKey(HttpPut putRequest,
      CloseableHttpClient httpClient) throws OzoneException, IOException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(putRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return;
      }

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      throw OzoneException.parse(EntityUtils.toString(entity));
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Gets a key from the Ozone server and writes to the file pointed by the
   * downloadTo PAth.
   *
   * @param keyName    - Key Name in Ozone.
   * @param downloadTo File Name to download the Key's Data to
   */
  public void getKey(String keyName, Path downloadTo) throws OzoneException {

    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    if (downloadTo == null) {
      throw new OzoneRestClientException("Invalid download path");
    }

    FileOutputStream outPutFile = null;
    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      outPutFile = new FileOutputStream(downloadTo.toFile());

      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      getRequest = getVolume().getClient().getHttpGet(builder.toString());
      executeGetKey(getRequest, httpClient, outPutFile);
      outPutFile.flush();
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      IOUtils.closeStream(outPutFile);
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * Returns the data part of the key as a string.
   *
   * @param keyName - KeyName to get
   * @return String - Data
   * @throws OzoneException
   */
  public String getKey(String keyName) throws OzoneException {

    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    HttpGet getRequest = null;
    ByteArrayOutputStream outPutStream = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      outPutStream = new ByteArrayOutputStream();

      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());

      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      getRequest = getVolume().getClient().getHttpGet(builder.toString());
      executeGetKey(getRequest, httpClient, outPutStream);
      return outPutStream.toString(ENCODING_NAME);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      IOUtils.closeStream(outPutStream);
      OzoneClientUtils.releaseConnection(getRequest);
    }

  }

  /**
   * Executes get key and returns the data.
   *
   * @param getRequest - http Get Request
   * @param httpClient - Client
   * @param stream     - Stream to write data to.
   * @throws IOException
   * @throws OzoneException
   */
  public static void executeGetKey(HttpGet getRequest,
      CloseableHttpClient httpClient, OutputStream stream)
      throws IOException, OzoneException {

    HttpEntity entity = null;
    try {

      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (errorCode == HTTP_OK) {
        entity.writeTo(stream);
        return;
      }

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      throw OzoneException.parse(EntityUtils.toString(entity));
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Deletes a key in this bucket.
   *
   * @param keyName - Name of the Key
   * @throws OzoneException
   */
  public void deleteKey(String keyName) throws OzoneException {

    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    HttpDelete deleteRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      deleteRequest = getVolume()
          .getClient().getHttpDelete(builder.toString());
      executeDeleteKey(deleteRequest, httpClient);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(deleteRequest);
    }
  }

  /**
   * Executes deleteKey.
   *
   * @param deleteRequest - http Delete Request
   * @param httpClient    - Client
   * @throws IOException
   * @throws OzoneException
   */
  private void executeDeleteKey(HttpDelete deleteRequest,
      CloseableHttpClient httpClient)
      throws IOException, OzoneException {

    HttpEntity entity = null;
    try {

      HttpResponse response = httpClient.execute(deleteRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (errorCode == HTTP_OK) {
        return;
      }

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      throw OzoneException.parse(EntityUtils.toString(entity));
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * List all keys in a bucket.
   *
   * @param resultLength The max length of listing result.
   * @param previousKey The key from where listing should start,
   *                    this key is excluded in the result.
   * @param prefix The prefix that return list keys start with.
   * @return List of OzoneKeys
   * @throws OzoneException
   */
  public List<OzoneKey> listKeys(String resultLength, String previousKey,
      String prefix) throws OzoneException {
    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      OzoneRestClient client = getVolume().getClient();
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName())
          .build();

      if (!Strings.isNullOrEmpty(resultLength)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_MAXKEYS, resultLength);
      }

      if (!Strings.isNullOrEmpty(previousKey)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREVKEY, previousKey);
      }

      if (!Strings.isNullOrEmpty(prefix)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix);
      }

      final String uri = builder.toString();
      getRequest = client.getHttpGet(uri);
      LOG.trace("listKeys URI={}", uri);
      return executeListKeys(getRequest, httpClient);

    } catch (IOException | URISyntaxException e) {
      throw new OzoneRestClientException(e.getMessage(), e);
    } finally {
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * List keys in a bucket with the provided prefix, with paging results.
   *
   * @param prefix The prefix of the object keys
   * @param maxResult max size per response
   * @param prevKey the previous key for paging
   */
  public List<OzoneKey> listKeys(String prefix, int maxResult, String prevKey)
      throws OzoneException {
    HttpGet getRequest = null;
    try {
      final URI uri =  new URIBuilder(volume.getClient().getEndPointURI())
          .setPath(OzoneConsts.KSM_KEY_PREFIX + getVolume().getVolumeName() +
              OzoneConsts.KSM_KEY_PREFIX + getBucketName())
          .setParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix)
          .setParameter(Header.OZONE_LIST_QUERY_MAXKEYS,
              String.valueOf(maxResult))
          .setParameter(Header.OZONE_LIST_QUERY_PREVKEY, prevKey)
          .build();
      final OzoneRestClient client = getVolume().getClient();
      getRequest = client.getHttpGet(uri.toString());
      return executeListKeys(getRequest, HttpClientBuilder.create().build());
    } catch (IOException | URISyntaxException e) {
      throw new OzoneRestClientException(e.getMessage());
    } finally {
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * Execute list Key.
   *
   * @param getRequest - HttpGet
   * @param httpClient - HttpClient
   * @return List<OzoneKey>
   * @throws IOException
   * @throws OzoneException
   */
  public static List<OzoneKey> executeListKeys(HttpGet getRequest,
      CloseableHttpClient httpClient) throws IOException, OzoneException {
    HttpEntity entity = null;
    List<OzoneKey> ozoneKeyList = new LinkedList<OzoneKey>();
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();

      entity = response.getEntity();

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }
      if (errorCode == HTTP_OK) {
        String temp = EntityUtils.toString(entity);
        ListKeys keyList = ListKeys.parse(temp);

        for (KeyInfo info : keyList.getKeyList()) {
          ozoneKeyList.add(new OzoneKey(info));
        }
        return ozoneKeyList;

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
   * Get info of the specified key.
   */
  public OzoneKey getKeyInfo(String keyName) throws OzoneException {
    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneRestClientException(
          "Unable to get key info, key name is null or empty");
    }

    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      OzoneRestClient client = getVolume().getClient();
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder
          .setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
              + "/" + keyName)
          .setParameter(Header.OZONE_LIST_QUERY_TAG,
              Header.OZONE_LIST_QUERY_KEY)
          .build();

      getRequest = client.getHttpGet(builder.toString());
      return executeGetKeyInfo(getRequest, httpClient);
    } catch (IOException | URISyntaxException e) {
      throw new OzoneRestClientException(e.getMessage(), e);
    } finally {
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * Execute get Key info.
   *
   * @param getRequest - HttpGet
   * @param httpClient - HttpClient
   * @return List<OzoneKey>
   * @throws IOException
   * @throws OzoneException
   */
  private OzoneKey executeGetKeyInfo(HttpGet getRequest,
      CloseableHttpClient httpClient) throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();
      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      if (errorCode == HTTP_OK) {
        OzoneKey key = new OzoneKey(
            KeyInfo.parse(EntityUtils.toString(entity)));
        return key;
      }
      throw OzoneException.parse(EntityUtils.toString(entity));
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }
}
