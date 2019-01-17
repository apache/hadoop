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
package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.header.AuthenticationHeaderParser;
import org.apache.hadoop.test.GenericTestUtils;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class test virtual host style mapping conversion to path style.
 */
public class TestVirtualHostStyleFilter {

  private static OzoneConfiguration conf;
  private static String s3HttpAddr;
  private AuthenticationHeaderParser authenticationHeaderParser;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    s3HttpAddr = "localhost:9878";
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, s3HttpAddr);
    s3HttpAddr = s3HttpAddr.substring(0, s3HttpAddr.lastIndexOf(":"));
    conf.set(S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME, s3HttpAddr);
    authenticationHeaderParser = new AuthenticationHeaderParser();
    authenticationHeaderParser.setAuthHeader("AWS ozone:scret");
  }

  /**
   * Create containerRequest object.
   * @return ContainerRequest
   * @throws Exception
   */
  public ContainerRequest createContainerRequest(String host, String path,
                                                 String queryParams,
                                                 boolean virtualHostStyle)
      throws Exception {
    URI baseUri = new URI("http://" + s3HttpAddr);
    URI virtualHostStyleUri;
    if (path == null && queryParams == null) {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr);
    } else if (path != null && queryParams == null) {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr + path);
    } else if (path !=null && queryParams != null)  {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr + path +
          queryParams);
    } else {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr  + queryParams);
    }
    URI pathStyleUri;
    if (queryParams == null) {
      pathStyleUri = new URI("http://" + s3HttpAddr + path);
    } else {
      pathStyleUri = new URI("http://" + s3HttpAddr + path + queryParams);
    }
    String httpMethod = "DELETE";
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    PropertiesDelegate propertiesDelegate = Mockito.mock(PropertiesDelegate
        .class);
    ContainerRequest containerRequest;
    if (virtualHostStyle) {
      containerRequest = new ContainerRequest(baseUri, virtualHostStyleUri,
          httpMethod, securityContext, propertiesDelegate);
      containerRequest.header(HttpHeaders.HOST, host);
    } else {
      containerRequest = new ContainerRequest(baseUri, pathStyleUri,
          httpMethod, securityContext, propertiesDelegate);
      containerRequest.header(HttpHeaders.HOST, host);
    }
    return containerRequest;
  }

  @Test
  public void testVirtualHostStyle() throws  Exception {
    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
            ".localhost:9878", "/myfile", null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/mybucket/myfile");
    Assert.assertEquals(expected, containerRequest.getRequestUri());
  }

  @Test
  public void testPathStyle() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest(s3HttpAddr,
        "/mybucket/myfile", null, false);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/mybucket/myfile");
    Assert.assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithCreateBucketRequest() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket");
    Assert.assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithQueryParams() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, "?prefix=bh", true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket?prefix=bh");
    assertTrue(expected.toString().contains(containerRequest.getRequestUri()
        .toString()));

    containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, "?prefix=bh&type=dir", true);
    virtualHostStyleFilter.filter(containerRequest);
    expected = new URI("http://" + s3HttpAddr +
        "/mybucket?prefix=bh&type=dir");
    assertTrue(expected.toString().contains(containerRequest.getRequestUri()
        .toString()));

  }

  @Test
  public void testVirtualHostStyleWithNoMatchingDomain() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".myhost:9999", null, null, true);
    try {
      virtualHostStyleFilter.filter(containerRequest);
      fail("testVirtualHostStyleWithNoMatchingDomain");
    } catch (InvalidRequestException ex) {
      GenericTestUtils.assertExceptionContains("No matching domain", ex);
    }

  }

  @Test
  public void testIncorrectVirtualHostStyle() throws
      Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    virtualHostStyleFilter.setAuthenticationHeaderParser(
        authenticationHeaderParser);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        "localhost:9878", null, null, true);
    try {
      virtualHostStyleFilter.filter(containerRequest);
      fail("testIncorrectVirtualHostStyle failed");
    } catch (InvalidRequestException ex) {
      GenericTestUtils.assertExceptionContains("invalid format", ex);
    }

  }

}
