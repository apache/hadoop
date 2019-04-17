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
package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test encode/decode of the continue token.
 */
public class TestContinueToken {

  @Test
  public void encodeDecode() throws OS3Exception {
    ContinueToken ct = new ContinueToken("key1", "dir1");

    ContinueToken parsedToken =
        ContinueToken.decodeFromString(ct.encodeToString());

    Assert.assertEquals(ct, parsedToken);
  }

  @Test
  public void encodeDecodeNullDir() throws OS3Exception {
    ContinueToken ct = new ContinueToken("key1", null);

    ContinueToken parsedToken =
        ContinueToken.decodeFromString(ct.encodeToString());

    Assert.assertEquals(ct, parsedToken);
  }

}