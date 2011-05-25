/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

// TODO: cover more test cases
public class TestScan {
  @Test
  public void testAttributesSerialization() throws IOException {
    Scan scan = new Scan();
    scan.setAttribute("attribute1", Bytes.toBytes("value1"));
    scan.setAttribute("attribute2", Bytes.toBytes("value2"));
    scan.setAttribute("attribute3", Bytes.toBytes("value3"));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteArrayOutputStream);
    scan.write(out);

    Scan scan2 = new Scan();
    Assert.assertTrue(scan2.getAttributesMap().isEmpty());

    scan2.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));

    Assert.assertNull(scan2.getAttribute("absent"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan2.getAttribute("attribute1")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan2.getAttribute("attribute2")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value3"), scan2.getAttribute("attribute3")));
    Assert.assertEquals(3, scan2.getAttributesMap().size());
  }

  @Test
  public void testScanAttributes() {
    Scan scan = new Scan();
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttribute("absent"));

    scan.setAttribute("absent", null);
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttribute("absent"));

    // adding attribute
    scan.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan.getAttribute("attribute1")));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan.getAttributesMap().get("attribute1")));

    // overriding attribute value
    scan.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), scan.getAttribute("attribute1")));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), scan.getAttributesMap().get("attribute1")));

    // adding another attribute
    scan.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan.getAttribute("attribute2")));
    Assert.assertEquals(2, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan.getAttributesMap().get("attribute2")));

    // removing attribute
    scan.setAttribute("attribute2", null);
    Assert.assertNull(scan.getAttribute("attribute2"));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertNull(scan.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    scan.setAttribute("attribute2", null);
    Assert.assertNull(scan.getAttribute("attribute2"));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertNull(scan.getAttributesMap().get("attribute2"));

    // removing another attribute
    scan.setAttribute("attribute1", null);
    Assert.assertNull(scan.getAttribute("attribute1"));
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttributesMap().get("attribute1"));
  }
}
