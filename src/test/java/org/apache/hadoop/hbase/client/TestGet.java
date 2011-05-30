/**
 * Copyright 2011 The Apache Software Foundation
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
public class TestGet {
  @Test
  public void testAttributesSerialization() throws IOException {
    Get get = new Get();
    get.setAttribute("attribute1", Bytes.toBytes("value1"));
    get.setAttribute("attribute2", Bytes.toBytes("value2"));
    get.setAttribute("attribute3", Bytes.toBytes("value3"));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteArrayOutputStream);
    get.write(out);

    Get get2 = new Get();
    Assert.assertTrue(get2.getAttributesMap().isEmpty());

    get2.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));

    Assert.assertNull(get2.getAttribute("absent"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get2.getAttribute("attribute1")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get2.getAttribute("attribute2")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value3"), get2.getAttribute("attribute3")));
    Assert.assertEquals(3, get2.getAttributesMap().size());
  }

  @Test
  public void testGetAttributes() {
    Get get = new Get();
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttribute("absent"));

    get.setAttribute("absent", null);
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttribute("absent"));

    // adding attribute
    get.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get.getAttribute("attribute1")));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get.getAttributesMap().get("attribute1")));

    // overriding attribute value
    get.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), get.getAttribute("attribute1")));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), get.getAttributesMap().get("attribute1")));

    // adding another attribute
    get.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get.getAttribute("attribute2")));
    Assert.assertEquals(2, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get.getAttributesMap().get("attribute2")));

    // removing attribute
    get.setAttribute("attribute2", null);
    Assert.assertNull(get.getAttribute("attribute2"));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertNull(get.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    get.setAttribute("attribute2", null);
    Assert.assertNull(get.getAttribute("attribute2"));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertNull(get.getAttributesMap().get("attribute2"));

    // removing another attribute
    get.setAttribute("attribute1", null);
    Assert.assertNull(get.getAttribute("attribute1"));
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttributesMap().get("attribute1"));
  }
}
