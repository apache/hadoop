/**
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Test QuotaCounts.
 */
public class TestQuotaCounts {
  @Test
  public void testBuildConstEnumCounters() throws Exception {
    QuotaCounts qc =
        new QuotaCounts.Builder().nameSpace(HdfsConstants.QUOTA_RESET)
            .storageSpace(HdfsConstants.QUOTA_RESET).build();
    // compare the references
    assertSame(QuotaCounts.QUOTA_RESET, qc.nsSsCounts);
    assertSame(QuotaCounts.STORAGE_TYPE_DEFAULT, qc.tsCounts);
    // compare the values
    assertEquals(HdfsConstants.QUOTA_RESET, qc.getNameSpace());
    assertEquals(HdfsConstants.QUOTA_RESET, qc.getStorageSpace());
    for (StorageType st : StorageType.values()) {
      assertEquals(0, qc.getTypeSpace(st));
    }
  }

  @Test
  public void testAddSpace() throws Exception {
    QuotaCounts qc = new QuotaCounts.Builder().build();
    qc.addNameSpace(1);
    qc.addStorageSpace(1024);
    assertEquals(1, qc.getNameSpace());
    assertEquals(1024, qc.getStorageSpace());
  }

  @Test
  public void testAdd() throws Exception {
    QuotaCounts qc1 = new QuotaCounts.Builder().build();
    QuotaCounts qc2 = new QuotaCounts.Builder().nameSpace(1).storageSpace(512)
        .typeSpaces(5).build();
    qc1.add(qc2);
    assertEquals(1, qc1.getNameSpace());
    assertEquals(512, qc1.getStorageSpace());
    for (StorageType type : StorageType.values()) {
      assertEquals(5, qc1.getTypeSpace(type));
    }
  }

  @Test
  public void testAddTypeSpaces() throws Exception {
    QuotaCounts qc = new QuotaCounts.Builder().build();
    for (StorageType t : StorageType.values()) {
      qc.addTypeSpace(t, 10);
    }
    for (StorageType type : StorageType.values()) {
      assertEquals(10, qc.getTypeSpace(type));
    }
  }

  @Test
  public void testSubtract() throws Exception {
    QuotaCounts qc1 = new QuotaCounts.Builder().build();
    QuotaCounts qc2 = new QuotaCounts.Builder().nameSpace(1).storageSpace(512)
        .typeSpaces(5).build();
    qc1.subtract(qc2);
    assertEquals(-1, qc1.getNameSpace());
    assertEquals(-512, qc1.getStorageSpace());
    for (StorageType type : StorageType.values()) {
      assertEquals(-5, qc1.getTypeSpace(type));
    }
  }

  @Test
  public void testSetTypeSpaces() throws Exception {
    QuotaCounts qc1 = new QuotaCounts.Builder().build();
    QuotaCounts qc2 = new QuotaCounts.Builder().nameSpace(1).storageSpace(512)
        .typeSpaces(5).build();
    qc1.setTypeSpaces(qc2.getTypeSpaces());
    for (StorageType t : StorageType.values()) {
      assertEquals(qc2.getTypeSpace(t), qc1.getTypeSpace(t));
    }

    // test ConstEnumCounters
    qc1.setTypeSpaces(QuotaCounts.STORAGE_TYPE_RESET);
    assertSame(QuotaCounts.STORAGE_TYPE_RESET, qc1.tsCounts);
  }

  @Test
  public void testSetSpaces() {
    QuotaCounts qc = new QuotaCounts.Builder().build();
    qc.setNameSpace(10);
    qc.setStorageSpace(1024);
    assertEquals(10, qc.getNameSpace());
    assertEquals(1024, qc.getStorageSpace());

    // test ConstEnumCounters
    qc.setNameSpace(HdfsConstants.QUOTA_RESET);
    qc.setStorageSpace(HdfsConstants.QUOTA_RESET);
    assertSame(QuotaCounts.QUOTA_RESET, qc.nsSsCounts);
  }

  @Test
  public void testNegation() throws Exception {
    QuotaCounts qc = new QuotaCounts.Builder()
        .nameSpace(HdfsConstants.QUOTA_RESET)
        .storageSpace(HdfsConstants.QUOTA_RESET)
        .typeSpaces(HdfsConstants.QUOTA_RESET).build();
    qc = qc.negation();
    assertEquals(1, qc.getNameSpace());
    assertEquals(1, qc.getStorageSpace());
    for (StorageType t : StorageType.values()) {
      assertEquals(1, qc.getTypeSpace(t));
    }
  }
}
