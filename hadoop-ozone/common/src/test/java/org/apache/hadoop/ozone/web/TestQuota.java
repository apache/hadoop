/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.web;

import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Volume Quota.
 */
public class TestQuota {
  @Test
  public void testParseQuota() {
    HashMap<String, Boolean> testMatrix;
    testMatrix = new HashMap<String, Boolean>();

    testMatrix.put("10TB", Boolean.TRUE);
    testMatrix.put("1 TB", Boolean.TRUE);
    testMatrix.put("0MB", Boolean.TRUE);
    testMatrix.put("0 TB", Boolean.TRUE);
    testMatrix.put("    1000MB   ", Boolean.TRUE);

    testMatrix.put("    1000MBMB   ", Boolean.FALSE);
    testMatrix.put("    1000MB00   ", Boolean.FALSE);
    testMatrix.put("1000ZMB", Boolean.FALSE);
    testMatrix.put("MB1000", Boolean.FALSE);
    testMatrix.put("9999", Boolean.FALSE);
    testMatrix.put("1", Boolean.FALSE);
    testMatrix.put("remove", Boolean.FALSE);
    testMatrix.put("1UNDEFINED", Boolean.FALSE);
    testMatrix.put(null, Boolean.FALSE);
    testMatrix.put("", Boolean.FALSE);
    testMatrix.put("-1000MB", Boolean.FALSE);
    testMatrix.put("1024 bytes", Boolean.TRUE);
    testMatrix.put("1bytes", Boolean.TRUE);
    testMatrix.put("0bytes", Boolean.TRUE);
    testMatrix.put("10000 BYTES", Boolean.TRUE);
    testMatrix.put("BYTESbytes", Boolean.FALSE);
    testMatrix.put("bytes", Boolean.FALSE);

    Set<String> keys = testMatrix.keySet();
    for (String key : keys) {
      if (testMatrix.get(key)) {
        OzoneQuota.parseQuota(key);
      } else {
        try {
          OzoneQuota.parseQuota(key);
          // should never get here since the isValid call will throw
          fail(key);
          fail("An exception was expected but did not happen.");
        } catch (IllegalArgumentException e) {

        }
      }
    }
  }

  @Test
  public void testVerifyQuota() {
    OzoneQuota qt = OzoneQuota.parseQuota("10TB");
    assertEquals(10, qt.getSize());
    assertEquals(OzoneQuota.Units.TB, qt.getUnit());
    assertEquals(10L * (1024L * 1024L * 1024L * 1024L), qt.sizeInBytes());

    qt = OzoneQuota.parseQuota("10MB");
    assertEquals(10, qt.getSize());
    assertEquals(OzoneQuota.Units.MB, qt.getUnit());
    assertEquals(10L * (1024L * 1024L), qt.sizeInBytes());

    qt = OzoneQuota.parseQuota("10GB");
    assertEquals(10, qt.getSize());
    assertEquals(OzoneQuota.Units.GB, qt.getUnit());
    assertEquals(10L * (1024L * 1024L * 1024L), qt.sizeInBytes());

    qt = OzoneQuota.parseQuota("10BYTES");
    assertEquals(10, qt.getSize());
    assertEquals(OzoneQuota.Units.BYTES, qt.getUnit());
    assertEquals(10L, qt.sizeInBytes());

    OzoneQuota emptyQuota = new OzoneQuota();
    assertEquals(-1L, emptyQuota.sizeInBytes());
    assertEquals(0, emptyQuota.getSize());
    assertEquals(OzoneQuota.Units.UNDEFINED, emptyQuota.getUnit());
  }

  @Test
  public void testVerifyRemove() {
    assertTrue(OzoneQuota.isRemove("remove"));
    assertFalse(OzoneQuota.isRemove("not remove"));
    assertFalse(OzoneQuota.isRemove(null));
  }
}
