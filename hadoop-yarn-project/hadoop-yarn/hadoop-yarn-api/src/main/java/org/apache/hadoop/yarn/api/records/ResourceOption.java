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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ResourceOption {

  public static ResourceOption newInstance(Resource resource,
      int overCommitTimeout){
    ResourceOption resourceOption = Records.newRecord(ResourceOption.class);
    resourceOption.setResource(resource);
    resourceOption.setOverCommitTimeout(overCommitTimeout);
    resourceOption.build();
    return resourceOption;
  }

  /**
   * Get the <em>resource</em> of the ResourceOption.
   * @return <em>resource</em> of the ResourceOption
   */
  @Private
  @Evolving
  public abstract Resource getResource();
  
  @Private
  @Evolving
  protected abstract void setResource(Resource resource);
  
  /**
   * Get timeout for tolerant of resource over-commitment
   * Note: negative value means no timeout so that allocated containers will
   * keep running until the end even under resource over-commitment cases.
   * @return <em>overCommitTimeout</em> of the ResourceOption
   */
  @Private
  @Evolving
  public abstract int getOverCommitTimeout();
  
  @Private
  @Evolving
  protected abstract void setOverCommitTimeout(int overCommitTimeout);
  
  @Private
  @Evolving
  protected abstract void build();
  
  @Override
  public String toString() {
    return "Resource:" + getResource().toString() 
        + ", overCommitTimeout:" + getOverCommitTimeout();
  }
  
}
