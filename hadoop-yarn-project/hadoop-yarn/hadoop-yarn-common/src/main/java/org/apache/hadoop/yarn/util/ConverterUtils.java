/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.util;

import static org.apache.hadoop.yarn.util.StringHelper._split;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


/**
 * This class contains a set of utilities which help converting data structures
 * from/to 'serializableFormat' to/from hadoop/nativejava data structures.
 *
 */
@Private
public class ConverterUtils {

  public static final String APPLICATION_PREFIX = "application";
  public static final String CONTAINER_PREFIX = "container";
  public static final String APPLICATION_ATTEMPT_PREFIX = "appattempt";

  /**
   * return a hadoop path from a given url
   * 
   * @param url
   *          url to convert
   * @return path from {@link URL}
   * @throws URISyntaxException
   */
  public static Path getPathFromYarnURL(URL url) throws URISyntaxException {
    String scheme = url.getScheme() == null ? "" : url.getScheme();
    
    String authority = "";
    if (url.getHost() != null) {
      authority = url.getHost();
      if (url.getUserInfo() != null) {
        authority = url.getUserInfo() + "@" + authority;
      }
      if (url.getPort() > 0) {
        authority += ":" + url.getPort();
      }
    }
    
    return new Path(
        (new URI(scheme, authority, url.getFile(), null, null)).normalize());
  }
  
  /**
   * change from CharSequence to string for map key and value
   * @param env map for converting
   * @return string,string map
   */
  public static Map<String, String> convertToString(
      Map<CharSequence, CharSequence> env) {
    
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Entry<CharSequence, CharSequence> entry: env.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
   }

  public static URL getYarnUrlFromPath(Path path) {
    return getYarnUrlFromURI(path.toUri());
  }
  
  public static URL getYarnUrlFromURI(URI uri) {
    URL url = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(URL.class);
    if (uri.getHost() != null) {
      url.setHost(uri.getHost());
    }
    if (uri.getUserInfo() != null) {
      url.setUserInfo(uri.getUserInfo());
    }
    url.setPort(uri.getPort());
    url.setScheme(uri.getScheme());
    url.setFile(uri.getPath());
    return url;
  }

  public static String toString(ApplicationId appId) {
    return appId.toString();
  }

  public static ApplicationId toApplicationId(RecordFactory recordFactory,
      String appIdStr) {
    Iterator<String> it = _split(appIdStr).iterator();
    it.next(); // prefix. TODO: Validate application prefix
    return toApplicationId(recordFactory, it);
  }

  private static ApplicationId toApplicationId(RecordFactory recordFactory,
      Iterator<String> it) {
    ApplicationId appId = ApplicationId.newInstance(Long.parseLong(it.next()),
        Integer.parseInt(it.next()));
    return appId;
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(Long.parseLong(it.next()),
        Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return appAttemptId;
  }

  private static ApplicationId toApplicationId(
      Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(Long.parseLong(it.next()),
        Integer.parseInt(it.next()));
    return appId;
  }

  public static String toString(ContainerId cId) {
    return cId == null ? null : cId.toString();
  }

  public static NodeId toNodeId(String nodeIdStr) {
    String[] parts = nodeIdStr.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid NodeId [" + nodeIdStr
          + "]. Expected host:port");
    }
    try {
      NodeId nodeId =
          NodeId.newInstance(parts[0], Integer.parseInt(parts[1]));
      return nodeId;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port: " + parts[1], e);
    }
  }

  public static ContainerId toContainerId(String containerIdStr) {
    return ContainerId.fromString(containerIdStr);
  }

  public static ApplicationAttemptId toApplicationAttemptId(
      String applicationAttmeptIdStr) {
    Iterator<String> it = _split(applicationAttmeptIdStr).iterator();
    if (!it.next().equals(APPLICATION_ATTEMPT_PREFIX)) {
      throw new IllegalArgumentException("Invalid AppAttemptId prefix: "
          + applicationAttmeptIdStr);
    }
    try {
      return toApplicationAttemptId(it);
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid AppAttemptId: "
          + applicationAttmeptIdStr, n);
    }
  }
  
  public static ApplicationId toApplicationId(
      String appIdStr) {
    Iterator<String> it = _split(appIdStr).iterator();
    if (!it.next().equals(APPLICATION_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
          + appIdStr + ". The valid ApplicationId should start with prefix "
          + APPLICATION_PREFIX);
    }
    try {
      return toApplicationId(it);
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid AppAttemptId: "
          + appIdStr, n);
    }
  }

  /**
   * Convert a protobuf token into a rpc token and set its service. Supposed
   * to be used for tokens other than RMDelegationToken. For
   * RMDelegationToken, use
   * {@link #convertFromYarn(org.apache.hadoop.yarn.api.records.Token,
   * org.apache.hadoop.io.Text)} instead.
   *
   * @param protoToken the yarn token
   * @param serviceAddr the connect address for the service
   * @return rpc token
   */
  public static <T extends TokenIdentifier> Token<T> convertFromYarn(
      org.apache.hadoop.yarn.api.records.Token protoToken,
      InetSocketAddress serviceAddr) {
    Token<T> token = new Token<T>(protoToken.getIdentifier().array(),
                                  protoToken.getPassword().array(),
                                  new Text(protoToken.getKind()),
                                  new Text(protoToken.getService()));
    if (serviceAddr != null) {
      SecurityUtil.setTokenService(token, serviceAddr);
    }
    return token;
  }

  /**
   * Convert a protobuf token into a rpc token and set its service.
   *
   * @param protoToken the yarn token
   * @param service the service for the token
   */
  public static <T extends TokenIdentifier> Token<T> convertFromYarn(
      org.apache.hadoop.yarn.api.records.Token protoToken,
      Text service) {
    Token<T> token = new Token<T>(protoToken.getIdentifier().array(),
        protoToken.getPassword().array(),
        new Text(protoToken.getKind()),
        new Text(protoToken.getService()));

    if (service != null) {
      token.setService(service);
    }
    return token;
  }
}
