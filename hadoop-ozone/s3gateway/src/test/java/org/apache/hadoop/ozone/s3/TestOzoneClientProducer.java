/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.s3.AWSAuthParser.AUTHORIZATION_HEADER;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.CONTENT_MD5;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.CONTENT_TYPE;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.HOST_HEADER;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.X_AMAZ_DATE;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.X_AMZ_CONTENT_SHA256;

/**
 * Test class for @{@link OzoneClientProducer}.
 * */
@RunWith(Parameterized.class)
public class TestOzoneClientProducer {

  private OzoneClientProducer producer;
  private MultivaluedMap<String, String> headerMap;
  private MultivaluedMap<String, String> queryMap;
  private String authHeader;
  private String contentMd5;
  private String host;
  private String amzContentSha256;
  private String date;
  private String contentType;


  private ContainerRequestContext context;
  private UriInfo uriInfo;

  public TestOzoneClientProducer(String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType)
      throws Exception {
    this.authHeader = authHeader;
    this.contentMd5 = contentMd5;
    this.host = host;
    this.amzContentSha256 = amzContentSha256;
    this.date = date;
    this.contentType = contentType;
    producer = new OzoneClientProducer();
    headerMap = new MultivaluedHashMap<>();
    queryMap = new MultivaluedHashMap<>();
    uriInfo = Mockito.mock(UriInfo.class);
    context = Mockito.mock(ContainerRequestContext.class);
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    setupContext();
    producer.setContext(context);
    producer.setOzoneConfiguration(config);
  }

  @Test
  public void testGetClientFailure() throws Exception {
    LambdaTestUtils.intercept(IOException.class, "Couldn't create" +
        " protocol ", () -> producer.createClient());
  }

  private void setupContext() throws Exception {
    headerMap.putSingle(AUTHORIZATION_HEADER, authHeader);
    headerMap.putSingle(CONTENT_MD5, contentMd5);
    headerMap.putSingle(HOST_HEADER, host);
    headerMap.putSingle(X_AMZ_CONTENT_SHA256, amzContentSha256);
    headerMap.putSingle(X_AMAZ_DATE, date);
    headerMap.putSingle(CONTENT_TYPE, contentType);

    Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryMap);
    Mockito.when(uriInfo.getRequestUri()).thenReturn(new URI(""));

    Mockito.when(context.getUriInfo()).thenReturn(uriInfo);
    Mockito.when(context.getHeaders()).thenReturn(headerMap);
    Mockito.when(context.getHeaderString(AUTHORIZATION_HEADER))
        .thenReturn(authHeader);
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryMap);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            "AWS4-HMAC-SHA256 Credential=testuser1/20190221/us-west-1/s3" +
                "/aws4_request, SignedHeaders=content-md5;host;" +
                "x-amz-content-sha256;x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Zi68x2nPDDXv5qfDC+ZWTg==",
            "s3g:9878",
            "e2bd43f11c97cde3465e0e8d1aad77af7ec7aa2ed8e213cd0e24" +
                "1e28375860c6",
            "20190221T002037Z",
            ""
        },
        {
            "AWS4-HMAC-SHA256 " +
                "Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request," +
                " SignedHeaders=content-type;host;x-amz-date, " +
                "Signature=" +
                "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400" +
                "e06b5924a6f2b5d7",
            "",
            "iam.amazonaws.com",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "20150830T123600Z",
            "application/x-www-form-urlencoded; charset=utf-8"
        }
    });
  }

}