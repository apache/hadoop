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
package org.apache.hadoop.hdfs;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/** Test {@link BlockStoragePolicy} */
public class TestBlockStoragePolicy {
  static final EnumSet<StorageType> none = EnumSet.noneOf(StorageType.class);
  static final EnumSet<StorageType> archive = EnumSet.of(StorageType.ARCHIVE);
  static final EnumSet<StorageType> disk = EnumSet.of(StorageType.DISK);
  static final EnumSet<StorageType> both = EnumSet.of(StorageType.DISK, StorageType.ARCHIVE);

  static {
    HdfsConfiguration.init();
  }

  @Test
  public void testDefaultPolicies() throws Exception {
    final byte COLD = (byte)4; 
    final byte WARM = (byte)8; 
    final byte HOT  = (byte)12; 
    final Map<Byte, String> expectedPolicyStrings = new HashMap<Byte, String>();
    expectedPolicyStrings.put(COLD,
        "BlockStoragePolicy{COLD:4, storageTypes=[ARCHIVE], creationFallbacks=[], replicationFallbacks=[]");
    expectedPolicyStrings.put(WARM,
        "BlockStoragePolicy{WARM:8, storageTypes=[DISK, ARCHIVE], creationFallbacks=[DISK, ARCHIVE], replicationFallbacks=[DISK, ARCHIVE]");
    expectedPolicyStrings.put(HOT,
        "BlockStoragePolicy{HOT:12, storageTypes=[DISK], creationFallbacks=[], replicationFallbacks=[ARCHIVE]");

    final Configuration conf = new Configuration();
    final BlockStoragePolicy[] policies = BlockStoragePolicy.readBlockStoragePolicies(conf);
    for(int i = 0; i < policies.length; i++) {
      if (policies[i] != null) {
        final String s = policies[i].toString();
        Assert.assertEquals(expectedPolicyStrings.get((byte)i), s);
      }
    }
    
    { // check Cold policy
      final BlockStoragePolicy cold = policies[COLD];
      for(short replication = 1; replication < 6; replication++) {
        final StorageType[] computed = cold.getStoragteTypes(replication);
        assertStorageType(computed, replication, StorageType.ARCHIVE);
      }
      assertCreationFallback(cold, null, null, null);
      assertReplicationFallback(cold, null, null, null);
    }
    
    { // check Warm policy
      final BlockStoragePolicy warm = policies[WARM];
      for(short replication = 1; replication < 6; replication++) {
        final StorageType[] computed = warm.getStoragteTypes(replication);
        assertStorageType(computed, replication, StorageType.DISK, StorageType.ARCHIVE);
      }
      assertCreationFallback(warm, StorageType.DISK, StorageType.DISK, StorageType.ARCHIVE);
      assertReplicationFallback(warm, StorageType.DISK, StorageType.DISK, StorageType.ARCHIVE);
    }

    { // check Hot policy
      final BlockStoragePolicy hot = policies[HOT];
      for(short replication = 1; replication < 6; replication++) {
        final StorageType[] computed = hot.getStoragteTypes(replication);
        assertStorageType(computed, replication, StorageType.DISK);
      }
      assertCreationFallback(hot, null, null, null);
      assertReplicationFallback(hot, StorageType.ARCHIVE, null, StorageType.ARCHIVE);
    }
  }

  static void assertStorageType(StorageType[] computed, short replication,
      StorageType... answers) {
    Assert.assertEquals(replication, computed.length);
    final StorageType last = answers[answers.length - 1];
    for(int i = 0; i < computed.length; i++) {
      final StorageType expected = i < answers.length? answers[i]: last;
      Assert.assertEquals(expected, computed[i]);
    }
  }

  static void assertCreationFallback(BlockStoragePolicy policy, StorageType noneExpected,
      StorageType archiveExpected, StorageType diskExpected) {
    Assert.assertEquals(noneExpected, policy.getCreationFallback(none));
    Assert.assertEquals(archiveExpected, policy.getCreationFallback(archive));
    Assert.assertEquals(diskExpected, policy.getCreationFallback(disk));
    Assert.assertEquals(null, policy.getCreationFallback(both));
  }

  static void assertReplicationFallback(BlockStoragePolicy policy, StorageType noneExpected,
      StorageType archiveExpected, StorageType diskExpected) {
    Assert.assertEquals(noneExpected, policy.getReplicationFallback(none));
    Assert.assertEquals(archiveExpected, policy.getReplicationFallback(archive));
    Assert.assertEquals(diskExpected, policy.getReplicationFallback(disk));
    Assert.assertEquals(null, policy.getReplicationFallback(both));
  }
}
