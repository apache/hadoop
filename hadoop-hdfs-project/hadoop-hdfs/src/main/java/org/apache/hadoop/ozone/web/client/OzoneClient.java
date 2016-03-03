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

import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.headers.Header;
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
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import javax.ws.rs.core.HttpHeaders;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
public class OzoneClient implements Closeable {
  private URI endPointURI;
  private String userAuth;

  /**
   * Constructor for OzoneClient.
   */
  public OzoneClient() {
  }

  /**
   * Constructor for OzoneClient.
   */
  public OzoneClient(String ozoneURI)
      throws OzoneException, URISyntaxException {
    setEndPoint(ozoneURI);
  }

  /**
   * Constructor for OzoneClient.
   */
  public OzoneClient(String ozoneURI, String userAuth)
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
      throw new OzoneClientException("Invalid ozone URI");
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
   * @throws OzoneClientException
   */
  public OzoneVolume createVolume(String volumeName, String onBehalfOf,
                                  String quota) throws OzoneException {
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();
      OzoneUtils.verifyBucketName(volumeName);

      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName);
      if (quota != null) {
        builder.setParameter(Header.OZONE_QUOTA_QUERY_TAG, quota);
      }

      HttpPost httppost = getHttpPost(onBehalfOf, builder.build().toString());
      executeCreateVolume(httppost, httpClient);
      return getVolume(volumeName);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneClientException(ex.getMessage());
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
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();

      OzoneUtils.verifyBucketName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName)
          .setParameter(Header.OZONE_LIST_QUERY_TAG,
              Header.OZONE_LIST_QUERY_VOLUME)
          .build();

      HttpGet httpget = getHttpGet(builder.toString());
      return executeInfoVolume(httpget, httpClient);

    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneClientException(ex.getMessage());
    }
  }

  /**
   * List all the volumes owned by the user or Owned by the user specified in
   * the behalf of string.
   *
   * @param onBehalfOf - User Name of the user if it is not the caller. for
   *                   example, an admin wants to list some other users
   *                   volumes.
   * @return List of Volumes
   * @throws OzoneException
   */
  public List<OzoneVolume> listVolumes(String onBehalfOf)
      throws OzoneException {
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();

      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/").build();

      HttpGet httpget = getHttpGet(builder.toString());
      if (onBehalfOf != null) {
        httpget.addHeader(Header.OZONE_USER, onBehalfOf);
      }
      return executeListVolume(httpget, httpClient);

    } catch (IOException | URISyntaxException ex) {
      throw new OzoneClientException(ex.getMessage());
    }
  }

  /**
   * delete a given volume.
   *
   * @param volumeName - volume to be deleted.
   * @throws OzoneException - Ozone Exception
   */
  public void deleteVolume(String volumeName) throws OzoneException {
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();

      OzoneUtils.verifyBucketName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName).build();

      HttpDelete httpdelete = getHttpDelete(builder.toString());
      executeDeleteVolume(httpdelete, httpClient);
    } catch (IOException | URISyntaxException | IllegalArgumentException ex) {
      throw new OzoneClientException(ex.getMessage());
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

    if (newOwner == null || newOwner.isEmpty()) {
      throw new OzoneClientException("Invalid new owner name");
    }
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();

      OzoneUtils.verifyBucketName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName).build();

      HttpPut putRequest = getHttpPut(builder.toString());
      putRequest.addHeader(Header.OZONE_USER, newOwner);
      executePutVolume(putRequest, httpClient);

    } catch (URISyntaxException | IllegalArgumentException | IOException ex) {
      throw new OzoneClientException(ex.getMessage());
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
      throw new OzoneClientException("Invalid quota");
    }
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();

      OzoneUtils.verifyBucketName(volumeName);
      URIBuilder builder = new URIBuilder(endPointURI);
      builder.setPath("/" + volumeName)
          .setParameter(Header.OZONE_QUOTA_QUERY_TAG, quota)
          .build();

      HttpPut putRequest = getHttpPut(builder.toString());
      executePutVolume(putRequest, httpClient);

    } catch (URISyntaxException | IllegalArgumentException | IOException ex) {
      throw new OzoneClientException(ex.getMessage());
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
                                   DefaultHttpClient httpClient)
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
        throw new OzoneClientException("Unexpected null in http payload");
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
                                        DefaultHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();

      entity = response.getEntity();
      if (entity == null) {
        throw new OzoneClientException("Unexpected null in http payload");
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
                                DefaultHttpClient httpClient)
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
                                              DefaultHttpClient httpClient)
      throws IOException, OzoneException {
    HttpEntity entity = null;
    List<OzoneVolume> volList = new LinkedList<>();
    try {
      HttpResponse response = httpClient.execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();
      entity = response.getEntity();

      if (entity == null) {
        throw new OzoneClientException("Unexpected null in http payload");
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
                                   DefaultHttpClient httpClient)
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
   * Returns a standard HttpPost Object to use for ozone post requests.
   *
   * @param onBehalfOf - If the use is being made on behalf of user, that user
   * @param uriString  - UriString
   * @return HttpPost
   */
  public HttpPost getHttpPost(String onBehalfOf, String uriString) {
    HttpPost httppost = new HttpPost(uriString);
    addOzoneHeaders(httppost);
    if (onBehalfOf != null) {
      httppost.addHeader(Header.OZONE_USER, onBehalfOf);
    }
    return httppost;
  }

  /**
   * Returns a standard HttpGet Object to use for ozone Get requests.
   *
   * @param uriString - The full Uri String
   * @return HttpGet
   */
  public HttpGet getHttpGet(String uriString) {
    HttpGet httpget = new HttpGet(uriString);
    addOzoneHeaders(httpget);
    return httpget;
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
}
