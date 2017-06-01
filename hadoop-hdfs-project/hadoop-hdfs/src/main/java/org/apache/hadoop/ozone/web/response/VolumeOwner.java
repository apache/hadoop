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

package org.apache.hadoop.ozone.web.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Volume Owner represents the owner of a volume.
 *
 * This is a class instead of a string since we might need to extend this class
 * to support other forms of authentication.
 */
@InterfaceAudience.Private
public class VolumeOwner {
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String name;

  /**
   * Constructor for VolumeOwner.
   *
   * @param name - name of the User
   */
  public VolumeOwner(String name) {
    this.name = name;
  }

  /**
   * Constructs Volume Owner.
   */
  public VolumeOwner() {
    name = null;
  }

  /**
   * Returns the user name.
   *
   * @return Name
   */
  public String getName() {
    return name;
  }

}
