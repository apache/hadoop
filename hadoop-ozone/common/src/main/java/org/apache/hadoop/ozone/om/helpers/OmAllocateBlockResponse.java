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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

/**
 * OM response for Allocated Block request.
 */
public class OmAllocateBlockResponse {
  private OmKeyInfo omKeyInfo;
  private OmKeyLocationInfo currentAllocatedBlockInfo;


  public OmAllocateBlockResponse(OmKeyInfo omKeyInfo,
      OmKeyLocationInfo omKeyLocationInfo) {
    this.omKeyInfo = omKeyInfo;
    this.currentAllocatedBlockInfo = omKeyLocationInfo;
  }

  public OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  public OmKeyLocationInfo getCurrentAllocatedBlockInfo() {
    return currentAllocatedBlockInfo;
  }
}
