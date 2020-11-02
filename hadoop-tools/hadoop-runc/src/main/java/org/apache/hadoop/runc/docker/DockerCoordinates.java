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

package org.apache.hadoop.runc.docker;

import java.net.MalformedURLException;
import java.net.URL;

public class DockerCoordinates {
  private final String baseUrl;
  private final String host;
  private final String image;
  private final String imageName;
  private final String imageRef;

  public DockerCoordinates(String defaultRepo, String ref) {
    boolean hasHost = hasHost(ref);
    this.host = computeHost(ref, defaultRepo, hasHost);
    this.baseUrl = computeBaseUrl(ref, defaultRepo, hasHost);
    this.image = ref;
    this.imageName = computeImageName(image, hasHost);
    this.imageRef = computeImageRef(image, hasHost);
  }

  private static String computeImageName(String ref, boolean hasHost) {
    if (hasHost) {
      ref = ref.substring(ref.indexOf("/") + 1);
    }

    if (ref.contains("@")) {
      return ref.substring(0, ref.indexOf('@'));
    }
    if (ref.contains(":")) {
      return ref.substring(0, ref.indexOf(':'));
    }

    return ref;
  }

  private static String computeImageRef(String ref, boolean hasHost) {
    if (hasHost) {
      ref = ref.substring(ref.indexOf("/") + 1);
    }
    if (ref.contains("@")) {
      return ref.substring(ref.indexOf('@') + 1);
    }
    if (ref.contains(":")) {
      return ref.substring(ref.indexOf(':') + 1);
    }
    return "latest";
  }

  private static boolean hasHost(String ref) {
    if (!ref.contains("/")) {
      return false;
    }
    String[] parts = ref.split("/", 2);
    String host = parts[0];
    return host.contains(".");
  }

  private static String computeHost(
      String ref, String defaultRepo, boolean hasHost) {
    return hasHost ? ref.split("/", 2)[0] : defaultRepo;
  }

  private static String computeBaseUrl(
      String ref, String defaultRepo, boolean hasHost) {

    String host = hasHost ? ref.split("/", 2)[0] : defaultRepo;

    int port = 443;
    if (host.contains(":")) {
      String sPort = host.substring(host.indexOf(":") + 1);
      host = host.substring(0, host.indexOf(":"));
      try {
        port = Integer.parseInt(sPort, 10);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Invalid port number: %s", sPort));
      }
    }
    try {
      String proto = "https";
      // use HTTP if IPv4 address specified
      if (host.matches("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")) {
        proto = "http";
      }

      if (port != 443) {
        return new URL(proto, host, port, "/v2/").toExternalForm();
      } else {
        return new URL(proto, host, "/v2/").toExternalForm();
      }
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          String.format("Invalid docker host: %s", host));
    }
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public String getImage() {
    return image;
  }

  public String getImageName() {
    return imageName;
  }

  public String getImageRef() {
    return imageRef;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return String.format(
        "docker-coordinates { "
            + "baseUrl=%s, host=%s, image=%s, imageName=%s, imageRef=%s }",
        baseUrl, host, image, imageName, imageRef);
  }
}
