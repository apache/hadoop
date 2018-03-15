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

package org.apache.hadoop.ozone.web;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.ozone.web.utils.OzoneUtils.getRequestID;
import static org.apache.hadoop.ozone.web.utils.OzoneUtils.verifyResourceName;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Utility operations like verifying resource name.
 */
public class TestUtils {

  /**
   * Tests if the bucket name handling is correct.
   */
  @Test
  public  void testValidBucketNames() {
    HashMap<String, Boolean> testMatrix;
    // Init the Table with Strings and Expected Return values
    testMatrix = new HashMap<String, Boolean>();

    testMatrix.put("bucket-.ozone.self", Boolean.FALSE);
    testMatrix.put("bucket.-ozone.self", Boolean.FALSE);
    testMatrix.put(".bucket.ozone.self", Boolean.FALSE);
    testMatrix.put("bucket.ozone.self.", Boolean.FALSE);
    testMatrix.put("bucket..ozone.self", Boolean.FALSE);
    testMatrix.put("192.1.1.1", Boolean.FALSE);
    testMatrix.put("ab", Boolean.FALSE);
    testMatrix.put("bucket.ozone.self.this.is.a.really.long.name.that."
        + "is.more.than.sixty.three.characters.long.for.sure", Boolean.FALSE);
    testMatrix.put(null, Boolean.FALSE);
    testMatrix.put("bucket@$", Boolean.FALSE);
    testMatrix.put("BUCKET", Boolean.FALSE);
    testMatrix.put("bucket .ozone.self", Boolean.FALSE);
    testMatrix.put("       bucket.ozone.self", Boolean.FALSE);
    testMatrix.put("bucket.ozone.self-", Boolean.FALSE);
    testMatrix.put("-bucket.ozone.self", Boolean.FALSE);

    testMatrix.put("bucket", Boolean.TRUE);
    testMatrix.put("bucket.ozone.self", Boolean.TRUE);
    testMatrix.put("bucket.ozone.self", Boolean.TRUE);
    testMatrix.put("bucket-name.ozone.self", Boolean.TRUE);
    testMatrix.put("bucket.1.ozone.self", Boolean.TRUE);

    Set<String> keys = testMatrix.keySet();
    for (String key : keys) {
      if (testMatrix.get(key)) {

        // For valid names there should be no exceptions at all
        verifyResourceName(key);
      } else {
        try {
          verifyResourceName(key);
          // should never get here since the isValid call will throw
          fail("An exception was expected but did not happen.");
        } catch (IllegalArgumentException e) {

        }
      }
    }
  }

  /**
   *  Just calls Request ID many times and assert we
   *  got different values, ideally this should be
   *  run under parallel threads. Since the function under
   *  test has no external dependencies it is assumed
   *  that this test is good enough.
   */
  @Test
  public void testRequestIDisRandom() {
    HashSet<String> set = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      assertTrue(set.add(getRequestID()));
    }
  }
}
