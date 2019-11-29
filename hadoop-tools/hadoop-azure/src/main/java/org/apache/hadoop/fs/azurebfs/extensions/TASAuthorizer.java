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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.permission.FsAction;

import java.io.IOException;
import java.net.URL;

/**
 * Implements Authorizer that can talk to Ranger Trusted Authorizer Service
 * TODO: Implementation
 */
public class TASAuthorizer implements AbfsAuthorizer {
  private boolean isAuthorized;
  private URL dSASUrl;

  @Override
  public void init() {
    isAuthorized = false;
    dSASUrl = null;
  }

  @Override
  public boolean isAuthorized(AbfsRestOperationType operationType,
      String relativePathFromAbfsFileSystemRoot) {
    return false;
  }

  @Override
  public String getSASToken() {
    return null;
  }

  public boolean isAuthorized(FsAction action, Path... absolutePaths) {
    // Trigger request to TAS
    // 1. Save isAuthorized
    // 2. Save DSAS Url
    /*
      Authorizer Request to TAS:
      ==========================
      POST authz/access/adls
      {
        "resource": {
          "storageaccount": "myaccount",
          "container":      "mycontainer",
          "relativepath":   "/user/john/data"
        },
        "acceessTypes": [
          "list"- accessTypes: read, add, create, write, delete, list, move,
      superuser
        ],
        "action": "list",- optional:action performed by user like mkdir,
      rmdir, rm,ls
        "context": { - optional: not used for now
        }
      }
      TAS Response to Authorizer:
      ===========================
      {
        "isAccessDetermined": true, - false – if there is no Ranger policy to
       make
      the authorization decision
        "isAllowed": true, - to be interpreted only when isAccessDetermined=true
        "additionalInfo": {
          "D-SAS": ""
        }
      }
    */

    if (isAuthorized && (dSASUrl == null)) {
      throw new RuntimeException("TODO: This was unexpected as respone parser "
          + "should have already thrown exception");
    }

    return isAuthorized;
  }

}
