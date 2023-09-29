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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** A collection of block storage policies. */
public class BlockStoragePolicySuite {
  static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicySuite
      .class);

  public static final String STORAGE_POLICY_XATTR_NAME
      = "hsm.block.storage.policy.id";
  public static final XAttr.NameSpace XAttrNS = XAttr.NameSpace.SYSTEM;

  public static final int ID_BIT_LENGTH = 4;

  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite() {
    return createDefaultSuite(null);
  }

  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite(
      final Configuration conf) {
    final BlockStoragePolicy[] policies =
        new BlockStoragePolicy[1 << ID_BIT_LENGTH];
    final byte lazyPersistId =
        HdfsConstants.StoragePolicy.LAZY_PERSIST.value();
    policies[lazyPersistId] = new BlockStoragePolicy(lazyPersistId,
        HdfsConstants.StoragePolicy.LAZY_PERSIST.name(),
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        true);    // Cannot be changed on regular files, but inherited.
    final byte allnvdimmId = HdfsConstants.StoragePolicy.ALL_NVDIMM.value();
    policies[allnvdimmId] = new BlockStoragePolicy(allnvdimmId,
        HdfsConstants.StoragePolicy.ALL_NVDIMM.name(),
        new StorageType[]{StorageType.NVDIMM},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte allssdId = HdfsConstants.StoragePolicy.ALL_SSD.value();
    policies[allssdId] = new BlockStoragePolicy(allssdId,
        HdfsConstants.StoragePolicy.ALL_SSD.name(),
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte onessdId = HdfsConstants.StoragePolicy.ONE_SSD.value();
    policies[onessdId] = new BlockStoragePolicy(onessdId,
        HdfsConstants.StoragePolicy.ONE_SSD.name(),
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK});
    final byte hotId = HdfsConstants.StoragePolicy.HOT.value();
    policies[hotId] = new BlockStoragePolicy(hotId,
        HdfsConstants.StoragePolicy.HOT.name(),
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});
    final byte warmId = HdfsConstants.StoragePolicy.WARM.value();
    policies[warmId] = new BlockStoragePolicy(warmId,
        HdfsConstants.StoragePolicy.WARM.name(),
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE});
    final byte coldId = HdfsConstants.StoragePolicy.COLD.value();
    policies[coldId] = new BlockStoragePolicy(coldId,
        HdfsConstants.StoragePolicy.COLD.name(),
        new StorageType[]{StorageType.ARCHIVE}, StorageType.EMPTY_ARRAY,
        StorageType.EMPTY_ARRAY);
    final byte providedId = HdfsConstants.StoragePolicy.PROVIDED.value();
    policies[providedId] = new BlockStoragePolicy(providedId,
      HdfsConstants.StoragePolicy.PROVIDED.name(),
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK});

    return new BlockStoragePolicySuite(getDefaultPolicyID(conf, policies),
        policies);
  }

  private static byte getDefaultPolicyID(
      final Configuration conf, final BlockStoragePolicy[] policies) {
    if (conf != null) {
      HdfsConstants.StoragePolicy defaultPolicy = conf.getEnum(
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY,
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT);
      for (BlockStoragePolicy policy : policies) {
        if (policy != null &&
            policy.getName().equalsIgnoreCase(defaultPolicy.name())) {
          return policy.getId();
        }
      }
    }
    return DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT.value();
  }

  private final byte defaultPolicyID;
  private final BlockStoragePolicy[] policies;

  public BlockStoragePolicySuite(byte defaultPolicyID,
      BlockStoragePolicy[] policies) {
    this.defaultPolicyID = defaultPolicyID;
    this.policies = policies;
  }

  /** @return the corresponding policy. */
  public BlockStoragePolicy getPolicy(byte id) {
    // id == 0 means policy not specified.
    return id == 0? getDefaultPolicy(): policies[id];
  }

  /** @return the default policy. */
  public BlockStoragePolicy getDefaultPolicy() {
    return getPolicy(defaultPolicyID);
  }

  public BlockStoragePolicy getPolicy(String policyName) {
    Preconditions.checkNotNull(policyName);

    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null && policy.getName().equalsIgnoreCase(policyName)) {
          return policy;
        }
      }
    }
    return null;
  }

  public BlockStoragePolicy[] getAllPolicies() {
    List<BlockStoragePolicy> list = Lists.newArrayList();
    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null) {
          list.add(policy);
        }
      }
    }
    return list.toArray(new BlockStoragePolicy[list.size()]);
  }

  public static String buildXAttrName() {
    return StringUtils.toLowerCase(XAttrNS.toString())
        + "." + STORAGE_POLICY_XATTR_NAME;
  }

  public static XAttr buildXAttr(byte policyId) {
    final String name = buildXAttrName();
    return XAttrHelper.buildXAttr(name, new byte[]{policyId});
  }

  public static String getStoragePolicyXAttrPrefixedName() {
    return XAttrHelper.getPrefixedName(XAttrNS, STORAGE_POLICY_XATTR_NAME);
  }

  public static boolean isStoragePolicyXAttr(XAttr xattr) {
    return xattr != null && xattr.getNameSpace() == XAttrNS
        && xattr.getName().equals(STORAGE_POLICY_XATTR_NAME);
  }
}
