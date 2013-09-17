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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Specific container requested back by the <code>ResourceManager</code>.
 * @see PreemptionContract
 * @see StrictPreemptionContract
 */
@Public
@Evolving
public abstract class PreemptionContainer {

  @Private
  @Unstable
  public static PreemptionContainer newInstance(ContainerId id) {
    PreemptionContainer container = Records.newRecord(PreemptionContainer.class);
    container.setId(id);
    return container;
  }

  /**
   * @return Container referenced by this handle.
   */
  @Public
  @Evolving
  public abstract ContainerId getId();

  @Private
  @Unstable
  public abstract void setId(ContainerId id);

}
