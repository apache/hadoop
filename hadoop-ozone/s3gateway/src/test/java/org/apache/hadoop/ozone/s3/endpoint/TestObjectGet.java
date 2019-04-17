/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test get object.
 */
public class TestObjectGet {

  public static final String CONTENT = "0123456789";

  @Test
  public void get() throws IOException, OS3Exception {
    //GIVEN
    OzoneClientStub client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bilbo", "b1");
    String volumeName = client.getObjectStore().getOzoneVolumeName("b1");
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket =
        volume.getBucket("b1");
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    ObjectEndpoint rest = new ObjectEndpoint();
    rest.setClient(client);
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    rest.setHeaders(headers);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    //WHEN
    Response response = rest.get("b1", "key1", null, 0, null, body);

    //THEN
    OzoneInputStream ozoneInputStream =
        volume.getBucket("b1")
            .readKey("key1");
    String keyContent =
        IOUtils.toString(ozoneInputStream, Charset.forName("UTF-8"));

    Assert.assertEquals(CONTENT, keyContent);
    Assert.assertEquals("" + keyContent.length(),
        response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

  }
}