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

package org.apache.hadoop.ozone.om.codec;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.fail;

/**
 * This class test S3SecretValueCodec.
 */
public class TestS3SecretValueCodec {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private S3SecretValueCodec codec;

  @Before
  public void initialize() {
    codec = new S3SecretValueCodec();
  }
  @Test
  public void testCodecWithCorrectData() throws Exception {

    S3SecretValue s3SecretValue =
        new S3SecretValue(UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    byte[] data = codec.toPersistedFormat(s3SecretValue);
    Assert.assertNotNull(data);

    S3SecretValue docdedS3Secret = codec.fromPersistedFormat(data);

    Assert.assertEquals(s3SecretValue, docdedS3Secret);

  }

  @Test
  public void testCodecWithIncorrectValues() throws Exception {
    try {
      codec.fromPersistedFormat("random".getBytes(StandardCharsets.UTF_8));
      fail("testCodecWithIncorrectValues failed");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Can't encode the the raw " +
          "data from the byte array", ex);
    }
  }

  @Test
  public void testCodecWithNullDataFromTable() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.fromPersistedFormat(null);
  }


  @Test
  public void testCodecWithNullDataFromUser() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.toPersistedFormat(null);
  }
}
