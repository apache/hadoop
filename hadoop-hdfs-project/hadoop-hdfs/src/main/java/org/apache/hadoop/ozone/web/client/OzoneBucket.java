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
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.headers.Header;
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import javax.ws.rs.core.HttpHeaders;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
   * Puts an Object in Ozone bucket.
   *
   * @param keyName - Name of the key
   * @param data    - Data that you want to put
   * @throws OzoneException
   */
  public void putKey(String keyName, String data) throws OzoneException {
    if ((keyName == null) || keyName.isEmpty()) {
      throw new OzoneClientException("Invalid key Name.");
    }

    if (data == null) {
      throw new OzoneClientException("Invalid data.");
    }

    try {
      OzoneClient client = getVolume().getClient();

      DefaultHttpClient httpClient = new DefaultHttpClient();

      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      HttpPut putRequest =
          getVolume().getClient().getHttpPut(builder.toString());

      InputStream is = new ByteArrayInputStream(data.getBytes(ENCODING));
      putRequest.setEntity(new InputStreamEntity(is, data.length()));
      is.mark(data.length());
      try {
        putRequest.setHeader(Header.CONTENT_MD5, DigestUtils.md5Hex(is));
      } finally {
        is.reset();
      }
      executePutKey(putRequest, httpClient);

    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
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
      throw new OzoneClientException("Invalid file object.");
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
      throw new OzoneClientException("Invalid key Name");
    }

    if (file == null) {
      throw new OzoneClientException("Invalid data stream");
    }

    try {
      OzoneClient client = getVolume().getClient();

      DefaultHttpClient httpClient = new DefaultHttpClient();
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      HttpPut putRequest =
          getVolume().getClient().getHttpPut(builder.toString());

      FileEntity fileEntity = new FileEntity(file, ContentType
          .APPLICATION_OCTET_STREAM);
      putRequest.setEntity(fileEntity);

      FileInputStream fis = new FileInputStream(file);
      putRequest.setHeader(Header.CONTENT_MD5, DigestUtils.md5Hex(fis));
      fis.close();
      putRequest.setHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(file
          .length()));
      httpClient.removeRequestInterceptorByClass(
          org.apache.http.protocol.RequestContent.class);
      executePutKey(putRequest, httpClient);

    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
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
  private void executePutKey(HttpPut putRequest, DefaultHttpClient httpClient)
      throws OzoneException, IOException {
    HttpEntity entity = null;
    try {

      HttpResponse response = httpClient.execute(putRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return;
      }

      if (entity == null) {
        throw new OzoneClientException("Unexpected null in http payload");
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
      throw new OzoneClientException("Invalid key Name");
    }

    if (downloadTo == null) {
      throw new OzoneClientException("Invalid download path");
    }

    try {
      OzoneClient client = getVolume().getClient();

      DefaultHttpClient httpClient = new DefaultHttpClient();
      FileOutputStream outPutFile = new FileOutputStream(downloadTo.toFile());

      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      HttpGet getRequest =
          getVolume().getClient().getHttpGet(builder.toString());
      executeGetKey(getRequest, httpClient, outPutFile);
      outPutFile.flush();
      outPutFile.close();
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
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
      throw new OzoneClientException("Invalid key Name");
    }

    try {
      OzoneClient client = getVolume().getClient();
      ByteArrayOutputStream outPutStream = new ByteArrayOutputStream();

      DefaultHttpClient httpClient = new DefaultHttpClient();
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());

      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      HttpGet getRequest =
          getVolume().getClient().getHttpGet(builder.toString());
      executeGetKey(getRequest, httpClient, outPutStream);
      return outPutStream.toString(ENCODING_NAME);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
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
  private void executeGetKey(HttpGet getRequest, DefaultHttpClient httpClient,
                             OutputStream stream)
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
        throw new OzoneClientException("Unexpected null in http payload");
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
      throw new OzoneClientException("Invalid key Name");
    }

    try {
      OzoneClient client = getVolume().getClient();
      DefaultHttpClient httpClient = new DefaultHttpClient();

      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName()
          + "/" + keyName).build();

      HttpDelete deleteRequest =
          getVolume().getClient().getHttpDelete(builder.toString());
      executeDeleteKey(deleteRequest, httpClient);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
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
                                DefaultHttpClient httpClient)
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
        throw new OzoneClientException("Unexpected null in http payload");
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
   * @return List of OzoneKeys
   */
  public List<OzoneKey> listKeys() throws OzoneException {
    try {
      OzoneClient client = getVolume().getClient();
      DefaultHttpClient httpClient = new DefaultHttpClient();
      URIBuilder builder = new URIBuilder(volume.getClient().getEndPointURI());
      builder.setPath("/" + getVolume().getVolumeName() + "/" + getBucketName())
          .build();

      HttpGet getRequest = client.getHttpGet(builder.toString());
      return executeListKeys(getRequest, httpClient);

    } catch (IOException | URISyntaxException e) {
      throw new OzoneClientException(e.getMessage());
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
  private List<OzoneKey> executeListKeys(HttpGet getRequest,
                                         DefaultHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    List<OzoneKey> ozoneKeyList = new LinkedList<OzoneKey>();
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int errorCode = response.getStatusLine().getStatusCode();

      entity = response.getEntity();

      if (entity == null) {
        throw new OzoneClientException("Unexpected null in http payload");
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
   * used to fix the content-length issue with protocol class.
   */
  private static class ContentLengthHeaderRemover implements
      HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context)
        throws IOException {
      request.removeHeaders(HTTP.CONTENT_LEN);
    }
  }
}
