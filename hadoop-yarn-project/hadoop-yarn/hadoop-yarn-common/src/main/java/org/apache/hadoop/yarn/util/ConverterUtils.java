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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
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


/**
 * This class contains a set of utilities which help converting data structures
 * from/to 'serializableFormat' to/from hadoop/nativejava data structures.
 *
 */
@Public
public class ConverterUtils {

  public static final String APPLICATION_PREFIX = "application";
  public static final String CONTAINER_PREFIX = "container";
  public static final String APPLICATION_ATTEMPT_PREFIX = "appattempt";

  /**
   * return a hadoop path from a given url
   * This method is deprecated, use {@link URL#toPath()} instead.
   * 
   * @param url
   *          url to convert
   * @return path from {@link URL}
   * @throws URISyntaxException
   */
  @Public
  @Deprecated
  public static Path getPathFromYarnURL(URL url) throws URISyntaxException {
    return url.toPath();
  }

  /*
   * This method is deprecated, use {@link URL#fromPath(Path)} instead.
   */
  @Public
  @Deprecated
  public static URL getYarnUrlFromPath(Path path) {
    return URL.fromPath(path);
  }
  
  /*
   * This method is deprecated, use {@link URL#fromURI(URI)} instead.
   */
  @Public
  @Deprecated
  public static URL getYarnUrlFromURI(URI uri) {
    return URL.fromURI(uri);
  }

  /*
   * This method is deprecated, use {@link ApplicationId#toString()} instead.
   */
  @Public
  @Deprecated
  public static String toString(ApplicationId appId) {
    return appId.toString();
  }

  /*
   * This method is deprecated, use {@link ApplicationId#fromString(String)}
   * instead.
   */
  @Public
  @Deprecated
  public static ApplicationId toApplicationId(RecordFactory recordFactory,
      String applicationIdStr) {
    return ApplicationId.fromString(applicationIdStr);
  }

  /*
   * This method is deprecated, use {@link ContainerId#toString()} instead.
   */
  @Public
  @Deprecated
  public static String toString(ContainerId cId) {
    return cId == null ? null : cId.toString();
  }

  @Private
  @InterfaceStability.Unstable
  public static NodeId toNodeIdWithDefaultPort(String nodeIdStr) {
    if (nodeIdStr.indexOf(":") < 0) {
      return NodeId.fromString(nodeIdStr + ":0");
    }
    return NodeId.fromString(nodeIdStr);
  }

  /*
   * This method is deprecated, use {@link NodeId#fromString(String)} instead.
   */
  @Public
  @Deprecated
  public static NodeId toNodeId(String nodeIdStr) {
    return NodeId.fromString(nodeIdStr);
  }

  /*
   * This method is deprecated, use {@link ContainerId#fromString(String)}
   * instead.
   */
  @Public
  @Deprecated
  public static ContainerId toContainerId(String containerIdStr) {
    return ContainerId.fromString(containerIdStr);
  }
  
  /*
   * This method is deprecated, use {@link ApplicationAttemptId#toString()}
   * instead.
   */
  @Public
  @Deprecated
  public static ApplicationAttemptId toApplicationAttemptId(
      String applicationAttemptIdStr) {
    return ApplicationAttemptId.fromString(applicationAttemptIdStr);
  }
  
  /*
   * This method is deprecated, use {@link ApplicationId#fromString(String)}
   * instead.
   */
  @Public
  @Deprecated
  public static ApplicationId toApplicationId(
      String appIdStr) {
    return ApplicationId.fromString(appIdStr);
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
