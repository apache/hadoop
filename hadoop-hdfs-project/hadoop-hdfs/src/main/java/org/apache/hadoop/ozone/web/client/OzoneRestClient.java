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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import javax.ws.rs.core.HttpHeaders;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Ozone client that connects to an Ozone server. Please note that this class is
 * not  thread safe.
 */
public class OzoneRestClient implements Closeable {
  private URI endPointURI;
  private String userAuth;

  /**
   * Constructor for OzoneRestClient.
   */
  public OzoneRestClient() {
  }

  /**
   * Constructor for OzoneRestClient.
   */
  public OzoneRestClient(String ozoneURI)
      throws OzoneException, URISyntaxException {
    setEndPoint(ozoneURI);
  }

  /**
   * Constructor for OzoneRestClient.
   */
  public OzoneRestClient(String ozoneURI, String userAuth)
      throws OzoneException, URISyntaxException {
    setEndPoint(ozoneURI);
    setUserAuth(userAuth);
  }

  /**
   * Returns the end Point.
   *
   * @return String
   */
  public URI getEndPointURI() {
    return endPointURI;
  }

  /**
   * Sets the End Point info using an URI.
   *
   * @param endPointURI - URI
   * @throws OzoneException
   */
  public void setEndPointURI(URI endPointURI) throws OzoneException {
    if ((endPointURI == null) || (endPointURI.toString().isEmpty())) {
      throw new OzoneRestClientException("Invalid ozone URI");
    }
    this.endPointURI = endPointURI;
  }

  /**
   * Set endPoint.
   *
   * @param clusterFQDN - cluster FQDN.
   */
  public void setEndPoint(String clusterFQDN) throws
      OzoneException, URISyntaxException {
    setEndPointURI(new URI(clusterFQDN));
  }

  /**
   * Get user Auth String.
   *
   * @return - User Auth String
   */
  public String getUserAuth() {
    return this.userAuth;
  }

  /**
   * Set User Auth.
   *
   * @param userAuth - User Auth String
   */
  public void setUserAuth(String userAuth) {
    this.userAuth = userAuth;
  }

  /**
   * create volume.
   *
   * @param volumeName - volume name 3 - 63 chars, small letters.
   * @param onBehalfOf - The user on behalf we are making the call for
   * @param quota      - Quota's are specified in a specific format. it is
   *                   integer(MB|GB|TB), for example 100TB.
   * @throws OzoneRestClientException
   */
  public OzoneVolume createVolume(String volumeName, String onBehalfOf,
                                  String quota) throws OzoneException {
    HttpPost httpPost = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(volumeName);

      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName);
      if (quota != null) {
        builder.setParameter(Header.OZONE_QUOTA_QUERY_TAG, quota);
      }

      httpPost = getHttpPost(onBehalfOf, builder.build().toString());
      executeCreateVolume(httpPost, httpClient);
      return getVolume(volumeName);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(httpPost);
    }
  }

  /**
   * Returns information about an existing Volume. if the Volume does not exist,
   * or if the user does not have access rights OzoneException is thrown
   *
   * @param volumeName - volume name 3 - 63 chars, small letters.
   * @return OzoneVolume Ozone Client Volume Class.
   * @throws OzoneException
   */
  public OzoneVolume getVolume(String volumeName) throws OzoneException {
    HttpGet httpGet = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName)
          .setParameter(Header.OZONE_INFO_QUERY_TAG,
              Header.OZONE_INFO_QUERY_VOLUME)
          .build();

      httpGet = getHttpGet(builder.toString());
      return executeInfoVolume(httpGet, httpClient);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(httpGet);
    }
  }

  /**
   * List all the volumes owned by the user or Owned by the user specified in
   * the behalf of string.
   *
   * @param onBehalfOf
   *  User Name of the user if it is not the caller. for example,
   *  an admin wants to list some other users volumes.
   * @param prefix
   *   Return only volumes that match this prefix.
   * @param maxKeys
   *   Maximum number of results to return, if the result set
   *   is smaller than requested size, it means that list is
   *   complete.
   * @param previousVolume
   *   The previous volume name.
   * @return List of Volumes
   * @throws OzoneException
   */
  public List<OzoneVolume> listVolumes(String onBehalfOf, String prefix,
      int maxKeys, String previousVolume) throws OzoneException {
    HttpGet httpGet = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      URIBuilder builder = new URIBuilder(endPointURI);
      if (!Strings.isNullOrEmpty(prefix)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix);
      }

      if (maxKeys > 0) {
        builder.addParameter(Header.OZONE_LIST_QUERY_MAXKEYS, Integer
            .toString(maxKeys));
      }

      if (!Strings.isNullOrEmpty(previousVolume)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREVKEY,
            previousVolume);
      }

      builder.setPath("/").build();

      httpGet = getHttpGet(builder.toString());
      if (onBehalfOf != null) {
        httpGet.addHeader(Header.OZONE_USER, onBehalfOf);
      }
      return executeListVolume(httpGet, httpClient);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(httpGet);
    }
  }

  /**
   * List all the volumes owned by the user or Owned by the user specified in
   * the behalf of string.
   *
   * @param onBehalfOf - User Name of the user if it is not the caller. for
   *                   example, an admin wants to list some other users
   *                   volumes.
   * @param prefix     - Return only volumes that match this prefix.
   * @param maxKeys    - Maximum number of results to return, if the result set
   *                   is smaller than requested size, it means that list is
   *                   complete.
   * @param prevKey    - The last key that client got, server will continue
   *                   returning results from that point.
   * @return List of Volumes
   * @throws OzoneException
   */
  public List<OzoneVolume> listVolumes(String onBehalfOf, String prefix,
      int maxKeys, OzoneVolume prevKey) throws OzoneException {
    String volumeName = null;

    if (prevKey != null) {
      volumeName = prevKey.getVolumeName();
    }

    return listVolumes(onBehalfOf, prefix, maxKeys, volumeName);
  }

  /**
   * List volumes of the current user or if onBehalfof is not null lists volume
   * owned by that user. You need admin privilege to read other users volume
   * lists.
   *
   * @param onBehalfOf - Name of the user you want to get volume list
   * @return - Volume list.
   * @throws OzoneException
   */
  public List<OzoneVolume> listVolumes(String onBehalfOf)
      throws OzoneException {
    return listVolumes(onBehalfOf, null,
        Integer.parseInt(Header.OZONE_DEFAULT_LIST_SIZE), StringUtils.EMPTY);
  }

  /**
   * List all volumes in a cluster. This can be invoked only by an Admin.
   *
   * @param prefix  - Returns only volumes that match this prefix.
   * @param maxKeys - Maximum niumber of keys to return
   * @param prevKey - Last Ozone Volume from the last Iteration.
   * @return List of Volumes
   * @throws OzoneException
   */
  public List<OzoneVolume> listAllVolumes(String prefix, int maxKeys,
      OzoneVolume prevKey) throws OzoneException {
    HttpGet httpGet = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      URIBuilder builder = new URIBuilder(endPointURI);
      if (prefix != null) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix);
      }

      if (maxKeys > 0) {
        builder.addParameter(Header.OZONE_LIST_QUERY_MAXKEYS, Integer
            .toString(maxKeys));
      }

      if (prevKey != null) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREVKEY,
            prevKey.getOwnerName()+ "/" + prevKey.getVolumeName());
      }

      builder.addParameter(Header.OZONE_LIST_QUERY_ROOTSCAN, "true");
      builder.setPath("/").build();
      httpGet = getHttpGet(builder.toString());
      return executeListVolume(httpGet, httpClient);

    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(httpGet);
    }
  }

    /**
     * delete a given volume.
     *
     * @param volumeName - volume to be deleted.
     * @throws OzoneException - Ozone Exception
     */
  public void deleteVolume(String volumeName) throws OzoneException {
    HttpDelete httpDelete = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName).build();

      httpDelete = getHttpDelete(builder.toString());
      executeDeleteVolume(httpDelete, httpClient);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(httpDelete);
    }
  }

  /**
   * Sets the Volume Owner.
   *
   * @param volumeName - Volume Name
   * @param newOwner   - New Owner Name
   * @throws OzoneException
   */
  public void setVolumeOwner(String volumeName, String newOwner)
      throws OzoneException {
    HttpPut putRequest = null;
    if (newOwner == null || newOwner.isEmpty()) {
      throw new OzoneRestClientException("Invalid new owner name");
    }
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName).build();

      putRequest = getHttpPut(builder.toString());
      putRequest.addHeader(Header.OZONE_USER, newOwner);
      executePutVolume(putRequest, httpClient);

    } catch (URISyntaxException | IllegalArgumentException | IOException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(putRequest);
    }
  }

  /**
   * Sets the Volume Quota. Quota's are specified in a specific format. it is
   * <integer>|(MB|GB|TB. for example 100TB.
   * <p>
   * To Remove a quota you can specify Header.OZONE_QUOTA_REMOVE
   *
   * @param volumeName - volume name
   * @param quota      - Quota String or  Header.OZONE_QUOTA_REMOVE
   * @throws OzoneException
   */
  public void setVolumeQuota(String volumeName, String quota)
      throws OzoneException {
    if (quota == null || quota.isEmpty()) {
      throw new OzoneRestClientException("Invalid quota");
    }
    HttpPut putRequest = null;
    try (CloseableHttpClient httpClient = newHttpClient()) {
      OzoneUtils.verifyResourceName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName)
          .setParameter(Header.OZONE_QUOTA_QUERY_TAG, quota)
          .build();

      putRequest = getHttpPut(builder.toString());
      executePutVolume(putRequest, httpClient);

    } catch (URISyntaxException | IllegalArgumentException | IOException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      OzoneClientUtils.releaseConnection(putRequest);
    }
  }

  /**
   * Sends the create Volume request to the server.
   *
   * @param httppost   - http post class
   * @param httpClient - httpClient
   * @throws IOException    -
   * @throws OzoneException
   */
  private void executeCreateVolume(HttpPost httppost,
      final CloseableHttpClient httpClient)
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
        EntityUtils.consume(entity);
      }
    }
  }

  /**
   * Sends the create Volume request to the server.
   *
   * @param httpGet - httpGet
   * @return OzoneVolume
   * @throws IOException    -
   * @throws OzoneException
   */
  private OzoneVolume executeInfoVolume(HttpGet httpGet,
      final CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();

      entity = response.getEntity();
      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      if (errorCode == HTTP_OK) {
        OzoneVolume volume = new OzoneVolume(this);
        volume.setVolumeInfo(EntityUtils.toString(entity));
        return volume;
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
   * Sends update volume requests to the server.
   *
   * @param putRequest http request
   * @throws IOException
   * @throws OzoneException
   */
  private void executePutVolume(HttpPut putRequest,
      final CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(putRequest);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();
      if (errorCode != HTTP_OK) {
        throw OzoneException.parse(EntityUtils.toString(entity));
      }
    } finally {
      if (entity != null) {
        EntityUtils.consume(entity);
      }
    }
  }

  /**
   * List Volumes.
   *
   * @param httpGet - httpGet
   * @return OzoneVolume
   * @throws IOException    -
   * @throws OzoneException
   */
  private List<OzoneVolume> executeListVolume(HttpGet httpGet,
      final CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    List<OzoneVolume> volList = new LinkedList<>();
    try {
      HttpResponse response = httpClient.execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (entity == null) {
        throw new OzoneRestClientException("Unexpected null in http payload");
      }

      String temp = EntityUtils.toString(entity);
      if (errorCode == HTTP_OK) {
        ListVolumes listVolumes =
            ListVolumes.parse(temp);

        for (VolumeInfo info : listVolumes.getVolumes()) {
          volList.add(new OzoneVolume(info, this));
        }
        return volList;

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
   * Delete Volume.
   *
   * @param httpDelete - Http Delete Request
   * @throws IOException
   * @throws OzoneException
   */
  private void executeDeleteVolume(HttpDelete httpDelete,
      final CloseableHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(httpDelete);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (errorCode != HTTP_OK) {
        throw OzoneException.parse(EntityUtils.toString(entity));
      }
    } finally {
      if (entity != null) {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Puts a Key in Ozone Bucket.
   *
   * @param volumeName - Name of the Volume
   * @param bucketName - Name of the Bucket
   * @param keyName - Name of the Key
   * @param file    - Stream that gets read to be put into Ozone.
   * @throws OzoneException
   */
  public void putKey(String volumeName, String bucketName, String keyName,
      File file) throws OzoneException {
    OzoneUtils.verifyResourceName(volumeName);
    OzoneUtils.verifyResourceName(bucketName);

    if (StringUtils.isEmpty(keyName)) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    if (file == null) {
      throw new OzoneRestClientException("Invalid data stream");
    }

    HttpPut putRequest = null;
    FileInputStream fis = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(getEndPointURI());
      builder.setPath("/" + volumeName + "/" + bucketName + "/" + keyName)
          .build();

      putRequest = getHttpPut(builder.toString());

      FileEntity fileEntity = new FileEntity(file, ContentType
          .APPLICATION_OCTET_STREAM);
      putRequest.setEntity(fileEntity);

      fis = new FileInputStream(file);
      putRequest.setHeader(Header.CONTENT_MD5, DigestUtils.md5Hex(fis));
      OzoneBucket.executePutKey(putRequest, httpClient);
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      IOUtils.closeStream(fis);
      OzoneClientUtils.releaseConnection(putRequest);
    }
  }

  /**
   * Gets a key from the Ozone server and writes to the file pointed by the
   * downloadTo Path.
   *
   * @param volumeName - Volume Name in Ozone.
   * @param bucketName - Bucket Name in Ozone.
   * @param keyName - Key Name in Ozone.
   * @param downloadTo File Name to download the Key's Data to
   */
  public void getKey(String volumeName, String bucketName, String keyName,
      Path downloadTo) throws OzoneException {
    OzoneUtils.verifyResourceName(volumeName);
    OzoneUtils.verifyResourceName(bucketName);

    if (StringUtils.isEmpty(keyName)) {
      throw new OzoneRestClientException("Invalid key Name");
    }

    if (downloadTo == null) {
      throw new OzoneRestClientException("Invalid download path");
    }

    FileOutputStream outPutFile = null;
    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      outPutFile = new FileOutputStream(downloadTo.toFile());

      URIBuilder builder = new URIBuilder(getEndPointURI());
      builder.setPath("/" + volumeName + "/" + bucketName + "/" + keyName)
          .build();

      getRequest = getHttpGet(builder.toString());
      OzoneBucket.executeGetKey(getRequest, httpClient, outPutFile);
      outPutFile.flush();
    } catch (IOException | URISyntaxException ex) {
      throw new OzoneRestClientException(ex.getMessage(), ex);
    } finally {
      IOUtils.closeStream(outPutFile);
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * List all keys in the given bucket.
   *
   * @param volumeName - Volume name
   * @param bucketName - Bucket name
   * @param resultLength The max length of listing result.
   * @param previousKey The key from where listing should start,
   *                    this key is excluded in the result.
   * @param prefix The prefix that return list keys start with.
   *
   * @return List of OzoneKeys
   */
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
      String resultLength, String previousKey, String prefix)
      throws OzoneException {
    OzoneUtils.verifyResourceName(volumeName);
    OzoneUtils.verifyResourceName(bucketName);

    HttpGet getRequest = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(getEndPointURI());
      builder.setPath("/" + volumeName + "/" + bucketName).build();

      if (!Strings.isNullOrEmpty(resultLength)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_MAXKEYS, resultLength);
      }

      if (!Strings.isNullOrEmpty(previousKey)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREVKEY, previousKey);
      }

      if (!Strings.isNullOrEmpty(prefix)) {
        builder.addParameter(Header.OZONE_LIST_QUERY_PREFIX, prefix);
      }

      getRequest = getHttpGet(builder.toString());
      return OzoneBucket.executeListKeys(getRequest, httpClient);
    } catch (IOException | URISyntaxException e) {
      throw new OzoneRestClientException(e.getMessage(), e);
    } finally {
      OzoneClientUtils.releaseConnection(getRequest);
    }
  }

  /**
   * Returns a standard HttpPost Object to use for ozone post requests.
   *
   * @param onBehalfOf - If the use is being made on behalf of user, that user
   * @param uriString  - UriString
   * @return HttpPost
   */
  public HttpPost getHttpPost(String onBehalfOf, String uriString) {
    HttpPost httpPost = new HttpPost(uriString);
    addOzoneHeaders(httpPost);
    if (onBehalfOf != null) {
      httpPost.addHeader(Header.OZONE_USER, onBehalfOf);
    }
    return httpPost;
  }

  /**
   * Returns a standard HttpGet Object to use for ozone Get requests.
   *
   * @param uriString - The full Uri String
   * @return HttpGet
   */
  public HttpGet getHttpGet(String uriString) {
    HttpGet httpGet = new HttpGet(uriString);
    addOzoneHeaders(httpGet);
    return httpGet;
  }

  /**
   * Returns httpDelete.
   *
   * @param uriString - uri
   * @return HttpDelete
   */
  public HttpDelete getHttpDelete(String uriString) {
    HttpDelete httpDel = new HttpDelete(uriString);
    addOzoneHeaders(httpDel);
    return httpDel;
  }

  /**
   * returns an HttpPut Object.
   *
   * @param uriString - Uri
   * @return HttpPut
   */
  public HttpPut getHttpPut(String uriString) {
    HttpPut httpPut = new HttpPut(uriString);
    addOzoneHeaders(httpPut);
    return httpPut;
  }

  /**
   * Add Ozone Headers.
   *
   * @param httpRequest - Http Request
   */
  private void addOzoneHeaders(HttpRequestBase httpRequest) {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);

    httpRequest.addHeader(Header.OZONE_VERSION_HEADER,
        Header.OZONE_V1_VERSION_HEADER);
    httpRequest.addHeader(HttpHeaders.DATE,
        format.format(new Date(Time.monotonicNow())));
    if (getUserAuth() != null) {
      httpRequest.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              getUserAuth());
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    // TODO : Currently we create a new HTTP client. We should switch
    // This to a Pool and cleanup the pool here.
  }

  @VisibleForTesting
  public CloseableHttpClient newHttpClient() {
    return OzoneClientUtils.newHttpClient();
  }
}
