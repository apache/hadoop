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

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test static utility methods of the ObjectEndpoint.
 */
public class TestObjectEndpoint {

  @Test
  public void parseSourceHeader() throws OS3Exception {
    Pair<String, String> bucketKey =
        ObjectEndpoint.parseSourceHeader("bucket1/key1");

    Assert.assertEquals("bucket1", bucketKey.getLeft());

    Assert.assertEquals("key1", bucketKey.getRight());
  }

  @Test
  public void parseSourceHeaderWithPrefix() throws OS3Exception {
    Pair<String, String> bucketKey =
        ObjectEndpoint.parseSourceHeader("/bucket1/key1");

    Assert.assertEquals("bucket1", bucketKey.getLeft());

    Assert.assertEquals("key1", bucketKey.getRight());
  }

}