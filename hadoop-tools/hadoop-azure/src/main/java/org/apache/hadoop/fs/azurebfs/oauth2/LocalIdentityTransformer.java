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
package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.utils.IdentityHandler;
import org.apache.hadoop.fs.azurebfs.utils.TextFileBasedIdentityHandler;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LOCAL_USER_SP_MAPPING_FILE_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LOCAL_GROUP_SG_MAPPING_FILE_PATH;


/**
 * A subclass of {@link IdentityTransformer} that translates the AAD to Local
 * identity using {@link IdentityHandler}.
 *
 * {@link TextFileBasedIdentityHandler} is a {@link IdentityHandler} implements
 * translation operation which returns identity mapped to AAD identity.
 */
public class LocalIdentityTransformer extends IdentityTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(LocalIdentityTransformer.class);

  private IdentityHandler localToAadIdentityLookup;

  public LocalIdentityTransformer(Configuration configuration) throws IOException {
    super(configuration);
    this.localToAadIdentityLookup =
        new TextFileBasedIdentityHandler(configuration.get(FS_AZURE_LOCAL_USER_SP_MAPPING_FILE_PATH),
            configuration.get(FS_AZURE_LOCAL_GROUP_SG_MAPPING_FILE_PATH));
  }

  /**
   * Perform identity transformation for the Get request results.
   * @param originalIdentity the original user or group in the get request results: FileStatus, AclStatus.
   * @param isUserName indicate whether the input originalIdentity is an owner name or owning group name.
   * @param localIdentity the local user or group, should be parsed from UserGroupInformation.
   * @return local identity.
   */
  @Override
  public String transformIdentityForGetRequest(String originalIdentity, boolean isUserName, String localIdentity)
      throws IOException {
    String localIdentityForOrig = isUserName ? localToAadIdentityLookup.lookupForLocalUserIdentity(originalIdentity)
        : localToAadIdentityLookup.lookupForLocalGroupIdentity(originalIdentity);

    if (localIdentityForOrig == null || localIdentityForOrig.isEmpty()) {
      return super.transformIdentityForGetRequest(originalIdentity, isUserName, localIdentity);
    }

    return localIdentityForOrig;
  }
}
