/*
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

package org.apache.hadoop.fs.s3a;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.audit.S3AAuditConstants;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

import static org.apache.hadoop.fs.s3a.Constants.CANNED_ACL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Tests of ACL handling in the FS.
 * If you enable logging, the grantee list adds
 * Grant [grantee=GroupGrantee [http://acs.amazonaws.com/groups/s3/LogDelivery], permission=WRITE]
 */
public class ITestS3ACannedACLs extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACannedACLs.class);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        CANNED_ACL);

    conf.set(CANNED_ACL, LOG_DELIVERY_WRITE);
    // needed because of direct calls made
    conf.setBoolean(S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS, false);
    return conf;
  }

  @Test
  public void testCreatedObjectsHaveACLs() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    assertObjectHasLoggingGrant(dir, false);
    Path path = new Path(dir, "1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasLoggingGrant(path, true);
    Path path2 = new Path(dir, "2");
    fs.rename(path, path2);
    assertObjectHasLoggingGrant(path2, true);
  }

  /**
   * Assert that a given object granted the AWS logging service
   * write access.
   * Logs all the grants.
   * @param path path
   * @param isFile is this a file or a directory?
   */
  private void assertObjectHasLoggingGrant(Path path, boolean isFile) {
    S3AFileSystem fs = getFileSystem();

    StoreContext storeContext = fs.createStoreContext();
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("acls");
    String key = storeContext.pathToKey(path);
    if (!isFile) {
      key = key + "/";
    }
    AccessControlList acl = s3.getObjectAcl(storeContext.getBucket(),
        key);
    List<Grant> grants = acl.getGrantsAsList();
    for (Grant grant : grants) {
      LOG.info("{}", grant.toString());
    }
    Grant loggingGrant = new Grant(GroupGrantee.LogDelivery, Permission.Write);
    Assertions.assertThat(grants)
        .describedAs("ACL grants of object %s", path)
        .contains(loggingGrant);
  }
}
