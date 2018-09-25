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
package org.apache.hadoop.ozone.s3.object;

import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test delete object.
 */
public class TestDeleteObject {

  @Test
  public void delete() throws IOException {
    //GIVEN
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createVolume("vol1");
    client.getObjectStore().getVolume("vol1").createBucket("b1");
    OzoneBucket bucket =
        client.getObjectStore().getVolume("vol1").getBucket("b1");
    bucket.createKey("key1", 0).close();

    DeleteObject rest = new DeleteObject();
    rest.setClient(client);

    //WHEN
    rest.delete("vol1", "b1", "key1");

    //THEN
    Assert.assertFalse("Bucket Should not contain any key after delete",
        bucket.listKeys("").hasNext());
  }
}