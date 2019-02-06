/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

/**
 * ScmInfo wraps the result returned from SCM#getScmInfo which
 * contains clusterId and the SCM Id.
 */
public final class ScmInfo {
  private String clusterId;
  private String scmId;

  /**
   * Builder for ScmInfo.
   */
  public static class Builder {
    private String clusterId;
    private String scmId;

    /**
     * sets the cluster id.
     * @param cid clusterId to be set
     * @return Builder for ScmInfo
     */
    public Builder setClusterId(String cid) {
      this.clusterId = cid;
      return this;
    }

    /**
     * sets the scmId.
     * @param id scmId
     * @return Builder for scmInfo
     */
    public Builder setScmId(String id) {
      this.scmId = id;
      return this;
    }

    public ScmInfo build() {
      return new ScmInfo(clusterId, scmId);
    }
  }

  private ScmInfo(String clusterId, String scmId) {
    this.clusterId = clusterId;
    this.scmId = scmId;
  }

  /**
   * Gets the clusterId from the Version file.
   * @return ClusterId
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Gets the SCM Id from the Version file.
   * @return SCM Id
   */
  public String getScmId() {
    return scmId;
  }
}
