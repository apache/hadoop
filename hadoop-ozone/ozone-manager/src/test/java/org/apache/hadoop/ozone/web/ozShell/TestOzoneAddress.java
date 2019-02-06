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

package org.apache.hadoop.ozone.web.ozShell;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.ozone.client.rest.OzoneException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test ozone URL parsing.
 */
@RunWith(Parameterized.class)
public class TestOzoneAddress {

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"o3fs://localhost:9878/"},
        {"o3fs://localhost/"},
        {"o3fs:///"},
        {"http://localhost:9878/"},
        {"http://localhost/"},
        {"http:///"},
        {"/"},
        {""}
    });
  }

  private String prefix;

  public TestOzoneAddress(String prefix) {
    this.prefix = prefix;
  }

  @Test
  public void checkUrlTypes() throws OzoneException, IOException {
    OzoneAddress address;

    address = new OzoneAddress("");
    address.ensureRootAddress();

    address = new OzoneAddress(prefix + "");
    address.ensureRootAddress();

    address = new OzoneAddress(prefix + "vol1");
    address.ensureVolumeAddress();
    Assert.assertEquals("vol1", address.getVolumeName());

    address = new OzoneAddress(prefix + "vol1/bucket");
    address.ensureBucketAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());

    address = new OzoneAddress(prefix + "vol1/bucket/");
    address.ensureBucketAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());

    address = new OzoneAddress(prefix + "vol1/bucket/key");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key/");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key/", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key1/key3/key");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key1/key3/key", address.getKeyName());
  }
}