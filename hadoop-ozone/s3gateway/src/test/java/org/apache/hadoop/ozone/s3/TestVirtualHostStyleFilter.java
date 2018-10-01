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

/**
 * This class test virtual host style mapping conversion to path style.
 */
public class TestVirtualHostStyleFilter {

  private static OzoneConfiguration conf;
  private static String s3HttpAddr;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    s3HttpAddr = "localhost:9878";
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, s3HttpAddr);
    conf.set(S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME, s3HttpAddr);
  }

  /**
   * Create containerRequest object.
   * @return ContainerRequest
   * @throws Exception
   */
  public ContainerRequest createContainerRequest(String host, String path,
                                                 boolean virtualHostStyle)
      throws Exception {
    URI baseUri = new URI("http://" + s3HttpAddr);
    URI virtualHostStyleUri;
    if (path == null) {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr);
    } else {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr + path);
    }
    URI pathStyleUri = new URI("http://" + s3HttpAddr + path);
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

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
            ".myvolume.localhost:9878", "/myfile", true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/myvolume/mybucket/myfile");
    Assert.assertEquals(expected, containerRequest.getRequestUri());
  }

  @Test
  public void testPathStyle() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest(s3HttpAddr,
        "/myvolume/mybucket/myfile", false);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/myvolume/mybucket/myfile");
    Assert.assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithCreateBucketRequest() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".myvolume.localhost:9878", null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/myvolume/mybucket");
    Assert.assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithNoMatchingDomain() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".myvolume.localhost:9999", null, true);
    try {
      virtualHostStyleFilter.filter(containerRequest);
    } catch (InvalidRequestException ex) {
      GenericTestUtils.assertExceptionContains("No matching domain", ex);
    }

  }

  @Test
  public void testVirtualHostStyleWithoutVolumeName() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest("mybucket." +
        ".localhost:9878", null, true);
    try {
      virtualHostStyleFilter.filter(containerRequest);
    } catch (InvalidRequestException ex) {
      GenericTestUtils.assertExceptionContains("invalid format", ex);
    }

  }

}
