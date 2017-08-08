/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rest;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Ozone REST Client Implementation, it connects Ozone Handler to execute
 * client calls. This uses REST protocol for the communication with server.
 */
public class OzoneRestClient implements OzoneClient, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneRestClient.class);

  private static final String SCHEMA = "http://";
  private static final int DEFAULT_OZONE_PORT = 50070;

  private final URI uri;
  private final UserGroupInformation ugi;
  private final OzoneAcl.OzoneACLRights userRights;
  private final OzoneAcl.OzoneACLRights groupRights;


  /**
   * Creates OzoneRpcClient instance with new OzoneConfiguration.
   *
   * @throws IOException
   */
  public OzoneRestClient() throws IOException, URISyntaxException {
    this(new OzoneConfiguration());
  }

   /**
    * Creates OzoneRpcClient instance with the given configuration.
    *
    * @param conf
    *
    * @throws IOException
    */
  public OzoneRestClient(Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.userRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_USER_RIGHTS,
        KSMConfigKeys.OZONE_KSM_USER_RIGHTS_DEFAULT);
    this.groupRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS,
        KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS_DEFAULT);

    //TODO: get uri from property ozone.reset.servers
    URIBuilder ozoneURI = null;
    try {
      ozoneURI = new URIBuilder(SCHEMA + "localhost");
      if (ozoneURI.getPort() == 0) {
        ozoneURI.setPort(DEFAULT_OZONE_PORT);
      }
      uri = ozoneURI.build();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createVolume(String volumeName)
      throws IOException {
    createVolume(volumeName, ugi.getUserName());
  }

  @Override
  public void createVolume(String volumeName, String owner)
      throws IOException {

    createVolume(volumeName, owner, OzoneConsts.MAX_QUOTA_IN_BYTES,
        (OzoneAcl[])null);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           OzoneAcl... acls)
      throws IOException {
    createVolume(volumeName, owner, OzoneConsts.MAX_QUOTA_IN_BYTES, acls);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           long quota)
      throws IOException {
    createVolume(volumeName, owner, quota, (OzoneAcl[])null);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           long quota, OzoneAcl... acls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(owner);
    Preconditions.checkNotNull(quota);
    Preconditions.checkState(quota >= 0);

    Set<OzoneAcl> aclSet = new HashSet<>();

    if(acls != null) {
      aclSet.addAll(Arrays.asList(acls));
    }

    LOG.info("Creating Volume: {}, with {} as owner and " +
        "quota set to {} bytes.", volumeName, owner, quota);
    HttpPost httpPost = null;
    HttpEntity entity = null;
    try (CloseableHttpClient httpClient = OzoneClientUtils.newHttpClient()) {
      URIBuilder builder = new URIBuilder(uri);
      builder.setPath("/" + volumeName);
      String quotaString = quota + Header.OZONE_QUOTA_BYTES;
      builder.setParameter(Header.OZONE_QUOTA_QUERY_TAG, quotaString);
      httpPost = getHttpPost(owner, builder.build().toString());
      for (OzoneAcl acl : aclSet) {
        httpPost.addHeader(
            Header.OZONE_ACLS, Header.OZONE_ACL_ADD + " " + acl.toString());
      }

      HttpResponse response = httpClient.execute(httpPost);
      entity = response.getEntity();
      int errorCode = response.getStatusLine().getStatusCode();
      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return;
      }
      if (entity != null) {
        throw new IOException(EntityUtils.toString(entity));
      } else {
        throw new IOException("Unexpected null in http payload");
      }
    } catch (URISyntaxException | IllegalArgumentException ex) {
      throw new IOException(ex.getMessage());
    } finally {
      EntityUtils.consume(entity);
      OzoneClientUtils.releaseConnection(httpPost);
    }
  }

  @Override
  public void setVolumeOwner(String volumeName, String owner)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(owner);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setVolumeQuota(String volumeName, long quota)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(quota);
    Preconditions.checkState(quota >= 0);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteVolume(String volumeName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Iterator<OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Iterator<OzoneVolume> listVolumes(String volumePrefix,
                                             String user)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createBucket(String volumeName, String bucketName)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        StorageType.DEFAULT, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           Versioning versioning)
      throws IOException {
    createBucket(volumeName, bucketName, versioning,
        StorageType.DEFAULT, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           StorageType storageType)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        storageType, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           OzoneAcl... acls)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        StorageType.DEFAULT, acls);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           Versioning versioning, StorageType storageType,
                           OzoneAcl... acls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(versioning);
    Preconditions.checkNotNull(storageType);

    String owner = ugi.getUserName();
    final List<OzoneAcl> listOfAcls = new ArrayList<>();

    //User ACL
    OzoneAcl userAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER,
            owner, userRights);
    listOfAcls.add(userAcl);

    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(owner).getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, group, groupRights)));

    //ACLs passed as argument
    if(acls != null) {
      Arrays.stream(acls).forEach((acl) -> listOfAcls.add(acl));
    }

    LOG.info("Creating Bucket: {}/{}, with Versioning {} and " +
        "Storage Type set to {}", volumeName, bucketName, versioning,
        storageType);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Converts OzoneConts.Versioning enum to boolean.
   *
   * @param version
   * @return corresponding boolean value
   */
  private boolean getBucketVersioningProtobuf(
      Versioning version) {
    if(version != null) {
      switch(version) {
      case ENABLED:
        return true;
      case NOT_DEFINED:
      case DISABLED:
      default:
        return false;
      }
    }
    return false;
  }

  @Override
  public void addBucketAcls(String volumeName, String bucketName,
                            List<OzoneAcl> addAcls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(addAcls);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void removeBucketAcls(String volumeName, String bucketName,
                               List<OzoneAcl> removeAcls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(removeAcls);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketVersioning(String volumeName, String bucketName,
                                  Versioning versioning)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(versioning);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketStorageType(String volumeName, String bucketName,
                                   StorageType storageType)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(storageType);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void checkBucketAccess(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName,
                                      String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Iterator<OzoneBucket> listBuckets(String volumeName,
                                            String bucketPrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName,
                                     String keyName, long size)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneInputStream getKey(String volumeName, String bucketName,
                                 String keyName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteKey(String volumeName, String bucketName,
                        String keyName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
                                 String keyPrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneKey getKeyDetails(String volumeName, String bucketName,
                                  String keyName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Converts Versioning to boolean.
   *
   * @param version
   * @return corresponding boolean value
   */
  private boolean getBucketVersioningFlag(
      Versioning version) {
    if(version != null) {
      switch(version) {
      case ENABLED:
        return true;
      case DISABLED:
      case NOT_DEFINED:
      default:
        return false;
      }
    }
    return false;
  }

  /**
   * Returns a standard HttpPost Object to use for ozone post requests.
   *
   * @param user - If the use is being made on behalf of user, that user
   * @param uriString  - UriString
   * @return HttpPost
   */
  public HttpPost getHttpPost(String user, String uriString) {
    HttpPost httpPost = new HttpPost(uriString);
    addOzoneHeaders(httpPost);
    if (user != null) {
      httpPost.addHeader(Header.OZONE_USER, user);
    }
    return httpPost;
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
    httpRequest.addHeader(HttpHeaders.AUTHORIZATION,
        Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
            ugi.getUserName());
  }

  @Override
  public void close() throws IOException {
  }
}
