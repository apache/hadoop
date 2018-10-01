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
package org.apache.hadoop.ozone.s3;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME;

/**
 * Filter used to convert virtual host style pattern to path style pattern.
 */

@Provider
@PreMatching
public class VirtualHostStyleFilter implements ContainerRequestFilter {

  private static final Logger LOG = LoggerFactory.getLogger(
      VirtualHostStyleFilter.class);
  private static final Pattern URL_SCHEME_PATTERN = Pattern.compile("" +
      "(?<bucket>(.+))\\.(?<volume>(.+))\\.");

  @Inject
  private OzoneConfiguration conf;
  private String[] domains;

  @Override
  public void filter(ContainerRequestContext requestContext) throws
      IOException {
    domains = conf.getTrimmedStrings(OZONE_S3G_DOMAIN_NAME);

    if (domains.length == 0) {
      // domains is not configured, might be it is path style.
      // So, do not continue further, just return.
      return;
    }
    //Get the value of the host
    String host = requestContext.getHeaderString(HttpHeaders.HOST);
    String domain = getDomainName(host);

    if (domain == null) {
      throw getException("Invalid S3 Gateway request {" + requestContext
          .getUriInfo().getRequestUri().toString() + " }: No matching domain " +
          "{" + Arrays.toString(domains) + "} for the host {" + host  + "}");
    }

    LOG.debug("Http header host name is {}", host);
    LOG.debug("Domain name matched is {}", domain);

    //Check if we have a Virtual Host style request, host length greater than
    // address length means it is virtual host style, we need to convert to
    // path style.
    if (host.length() > domain.length()) {
      String bothNames = host.substring(0, host.length() - domain.length());
      LOG.debug("Both volume name and bucket name is {}", bothNames);
      Matcher matcher = URL_SCHEME_PATTERN.matcher(bothNames);

      if (!matcher.matches()) {
        throw getException("Invalid S3 Gateway request {" + requestContext
            .getUriInfo().getRequestUri().toString() +"}:" +" Host: {" + host
            + " is in invalid format");
      }

      String bucketStr = matcher.group("bucket");
      String volumeStr = matcher.group("volume");

      LOG.debug("bucket {}, volumeStr {}", bucketStr, volumeStr);

      URI baseURI = requestContext.getUriInfo().getBaseUri();
      String currentPath = requestContext.getUriInfo().getPath();
      String newPath = String.format("%s/%s", volumeStr, bucketStr);
      if (currentPath != null) {
        newPath += String.format("%s", currentPath);
      }
      URI requestAddr = UriBuilder.fromUri(baseURI).path(newPath).build();
      requestContext.setRequestUri(baseURI, requestAddr);
    }
  }

  private InvalidRequestException getException(String message) {
    return new InvalidRequestException(message);
  }

  @VisibleForTesting
  public void setConfiguration(OzoneConfiguration config) {
    this.conf = config;
  }


  /**
   * This method finds the longest match with the domain name.
   * @param host
   * @return domain name matched with the host. if none of them are matching,
   * return null.
   */
  private String getDomainName(String host) {
    String match = null;
    int length=0;
    for (String domainVal : domains) {
      if (host.endsWith(domainVal)) {
        int len = domainVal.length();
        if (len > length) {
          length = len;
          match = domainVal;
        }
      }
    }
    return match;
  }

}
