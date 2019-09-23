/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class BaseResource implements Serializable {
  private static final long serialVersionUID = 1492603053176889431L;

  private String uri;

  /**
   * Resource location for a service, e.g.
   * /app/v1/services/helloworld
   *
   **/
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BaseResource [uri=")
        .append(uri)
        .append("]");
    return builder.toString();
  }
}
