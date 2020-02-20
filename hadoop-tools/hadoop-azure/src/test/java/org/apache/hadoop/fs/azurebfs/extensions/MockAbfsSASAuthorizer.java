/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.sasTokenGenerator.*;
import org.apache.hadoop.fs.azurebfs.services.*;

import java.io.*;
import java.time.*;
import java.util.*;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.*;
import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizerEnums.*;
import static org.apache.hadoop.fs.azurebfs.sasTokenGenerator.SASTokenConstants.*;

/**
 * A mock Azure Blob File System Authorization Implementation
 */
public class MockAbfsSASAuthorizer extends MockAbfsAuthorizer {
  public MockAbfsSASAuthorizer(Configuration conf) {
    super(conf);
    sasGenerator = new DelegationSASGenerator(delegationKey.Value,
        delegationKey.SignedOid, delegationKey.SignedTid,
        delegationKey.SignedStart, delegationKey.SignedExpiry,
        delegationKey.SignedService, delegationKey.SignedVersion);
  }

  //  public static final String TEST_READ_ONLY_FILE_0 = "readOnlyFile0";
//  public static final String TEST_READ_ONLY_FILE_1 = "readOnlyFile1";
//  public static final String TEST_READ_ONLY_FOLDER = "readOnlyFolder";
//  public static final String TEST_WRITE_ONLY_FILE_0 = "writeOnlyFile0";
//  public static final String TEST_WRITE_ONLY_FILE_1 = "writeOnlyFile1";
//  public static final String TEST_WRITE_ONLY_FOLDER = "writeOnlyFolder";
//  public static final String TEST_READ_WRITE_FILE_0 = "readWriteFile0";
//  public static final String TEST_READ_WRITE_FILE_1 = "readWriteFile1";
//  public static final String TEST_WRITE_THEN_READ_ONLY =
//      "writeThenReadOnlyFile";
//  private static final Set<String> apiAuthorizerActions = new HashSet<>();
  private static UserDelegationKey delegationKey = null;
  DelegationSASGenerator sasGenerator = null;
//  public String accountName;
//  private Configuration conf;
//  private Set<String> readOnlyPathsPrefixes = new HashSet<>();
//  private Set<String> writeOnlyPathsPrefixes = new HashSet<>();
//  private Set<String> readWritePathsPrefixes = new HashSet<>();
//
//  private Map<String, WriteReadMode> writeReadModeMap = null;
//
//  public MockAbfsSASAuthorizer(Configuration conf) {
//    this.conf = conf;
//  }

  public static UserDelegationKey getUserDelegationKey() {
    return delegationKey;
  }

  public static void setUserDelegationKey(UserDelegationKey userDelegationKey) {
    delegationKey = userDelegationKey;
  }

  @Override
  public AuthorizationResourceResult getAuthzResourceResult(AuthorizationResource resource)
      throws InvalidUriException, AbfsAuthorizerUnhandledException,
      AbfsAuthorizationException, UnsupportedEncodingException {
    return getAuthzResourceResultWithSAS(resource);
  }

  private AuthorizationResourceResult getAuthzResourceResultWithSAS(
      AuthorizationResource resource)
      throws InvalidUriException, AbfsAuthorizerUnhandledException,
      AbfsAuthorizationException, UnsupportedEncodingException {

    AuthorizationResourceResult resourceResult =
        new AuthorizationResourceResult(
            resource.getStorePathUri(),
            resource.getAuthorizerAction(),
            sasGenerator.generateSAS(
                true,
                resource.getAuthorizerAction(),
                resource.getStorePathUri(),
                Instant.now(),
                Instant.now().plusSeconds(60 * 6),
                java.util.UUID.randomUUID().toString(),
                VALID_SAS_REST_API_VERSIONS.iterator().next(),
                null,
                null,
                null));

    return resourceResult;
  }

  @Override
  public void init()
      throws AbfsAuthorizationException, AbfsAuthorizerUnhandledException {

  }

  @Override
  public AuthType getAuthType() {
    return AuthType.SAS;
  }
}
