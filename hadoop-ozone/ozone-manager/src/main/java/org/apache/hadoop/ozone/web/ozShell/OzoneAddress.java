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
package org.apache.hadoop.ozone.web.ozShell;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.rest.OzoneException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RPC_SCHEME;
import org.apache.http.client.utils.URIBuilder;

/**
 * Address of an ozone object for ozone shell.
 */
public class OzoneAddress {

  private static final String EMPTY_HOST = "___DEFAULT___";

  private URI ozoneURI;

  private String volumeName = "";

  private String bucketName = "";

  private String keyName = "";

  public OzoneAddress() throws OzoneException {
    this("o3:///");
  }

  public OzoneAddress(String address)
      throws OzoneException {
    if (address == null || address.equals("")) {
      address = OZONE_RPC_SCHEME + ":///";
    }
    this.ozoneURI = parseURI(address);
    String path = this.ozoneURI.getPath();

    path = path.replaceAll("^/+", "");

    int sep1 = path.indexOf('/');
    int sep2 = path.indexOf('/', sep1 + 1);

    if (sep1 == -1) {
      volumeName = path;
    } else {
      //we have vol/bucket
      volumeName = path.substring(0, sep1);
      if (sep2 == -1) {
        bucketName = path.substring(sep1 + 1);
      } else {
        //we have vol/bucket/key/.../...
        bucketName = path.substring(sep1 + 1, sep2);
        keyName = path.substring(sep2 + 1);
      }
    }

  }

  public OzoneClient createClient(OzoneConfiguration conf)
      throws IOException, OzoneClientException {
    OzoneClient client;
    String scheme = ozoneURI.getScheme();
    if (ozoneURI.getScheme() == null || scheme.isEmpty()) {
      scheme = OZONE_RPC_SCHEME;
    }
    if (scheme.equals(OZONE_HTTP_SCHEME)) {
      if (ozoneURI.getHost() != null && !ozoneURI.getAuthority()
          .equals(EMPTY_HOST)) {
        if (ozoneURI.getPort() == -1) {
          client = OzoneClientFactory.getRestClient(ozoneURI.getHost());
        } else {
          client = OzoneClientFactory
              .getRestClient(ozoneURI.getHost(), ozoneURI.getPort(), conf);
        }
      } else {
        client = OzoneClientFactory.getRestClient(conf);
      }
    } else if (scheme.equals(OZONE_RPC_SCHEME)) {
      if (ozoneURI.getHost() != null && !ozoneURI.getAuthority()
          .equals(EMPTY_HOST)) {
        if (ozoneURI.getPort() == -1) {
          client = OzoneClientFactory.getRpcClient(ozoneURI.getHost());
        } else {
          client = OzoneClientFactory
              .getRpcClient(ozoneURI.getHost(), ozoneURI.getPort(), conf);
        }
      } else {
        client = OzoneClientFactory.getRpcClient(conf);
      }
    } else {
      throw new OzoneClientException(
          "Invalid URI, unknown protocol scheme: " + scheme);
    }
    return client;
  }

  /**
   * verifies user provided URI.
   *
   * @param uri - UriString
   * @return URI
   * @throws URISyntaxException
   * @throws OzoneException
   */
  protected URI parseURI(String uri)
      throws OzoneException {
    if ((uri == null) || uri.isEmpty()) {
      throw new OzoneClientException(
          "Ozone URI is needed to execute this command.");
    }
    URIBuilder uriBuilder = new URIBuilder(stringToUri(uri));
    if (uriBuilder.getPort() == 0) {
      uriBuilder.setPort(Shell.DEFAULT_OZONE_PORT);
    }

    try {
      return uriBuilder.build();
    } catch (URISyntaxException e) {
      throw new OzoneClientException("Invalid URI: " + ozoneURI, e);
    }
  }

  /**
   * Construct a URI from a String with unescaped special characters
   * that have non-standard semantics. e.g. /, ?, #. A custom parsing
   * is needed to prevent misbehavior.
   *
   * @param pathString The input path in string form
   * @return URI
   */
  private static URI stringToUri(String pathString) {
    // parse uri components
    String scheme = null;
    String authority = null;
    int start = 0;

    // parse uri scheme, if any
    int colon = pathString.indexOf(':');
    int slash = pathString.indexOf('/');
    if (colon > 0 && (slash == colon + 1)) {
      // has a non zero-length scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length() - start > 2)) {
      start += 2;
      int nextSlash = pathString.indexOf('/', start);
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start, authEnd);
      start = authEnd;
    }
    // uri path is the rest of the string. ? or # are not interpreted,
    // but any occurrence of them will be quoted by the URI ctor.
    String path = pathString.substring(start, pathString.length());

    // add leading slash to the path, if it does not exist
    int firstSlash = path.indexOf('/');
    if(firstSlash != 0) {
      path = "/" + path;
    }

    if (authority == null || authority.equals("")) {
      authority = EMPTY_HOST;
    }
    // Construct the URI
    try {
      return new URI(scheme, authority, path, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public void ensureBucketAddress() throws OzoneClientException {
    if (keyName.length() > 0) {
      throw new OzoneClientException(
          "Invalid bucket name. Delimiters (/) not allowed in bucket name");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is required.");
    } else if (bucketName.length() == 0) {
      throw new OzoneClientException(
          "Bucket name is required.");
    }
  }

  public void ensureKeyAddress() throws OzoneClientException {
    if (keyName.length() == 0) {
      throw new OzoneClientException(
          "Key name is missing.");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is missing");
    } else if (bucketName.length() == 0) {
      throw new OzoneClientException(
          "Bucket name is missing");
    }
  }

  public void ensureVolumeAddress() throws OzoneClientException {
    if (keyName.length() != 0) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is required");
    } else if (bucketName.length() != 0) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    }
  }

  public void ensureRootAddress() throws OzoneClientException {
    if (keyName.length() != 0 || bucketName.length() != 0
        || volumeName.length() != 0) {
      throw new OzoneClientException(
          "Invalid URI. Volume/bucket/key elements should not been used");
    }
  }
}
