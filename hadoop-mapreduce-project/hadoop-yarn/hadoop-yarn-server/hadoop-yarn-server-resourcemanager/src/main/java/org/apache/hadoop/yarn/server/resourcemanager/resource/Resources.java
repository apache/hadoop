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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

@Private
@Evolving
public class Resources {
  // Java doesn't have const :(
  private static final Resource NONE = createResource(0);

  public static Resource createResource(int memory) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    return resource;
  }

  public static Resource none() {
    assert NONE.getMemory() == 0 : "NONE should be empty";
    return NONE;
  }

  public static Resource clone(Resource res) {
    return createResource(res.getMemory());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory());
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory());
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, int by) {
    lhs.setMemory(lhs.getMemory() * by);
    return lhs;
  }

  public static Resource multiply(Resource lhs, int by) {
    return multiplyTo(clone(lhs), by);
  }

  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.getMemory() == rhs.getMemory();
  }

  public static boolean lessThan(Resource lhs, Resource rhs) {
    return lhs.getMemory() < rhs.getMemory();
  }

  public static boolean lessThanOrEqual(Resource lhs, Resource rhs) {
    return lhs.getMemory() <= rhs.getMemory();
  }

  public static boolean greaterThan(Resource lhs, Resource rhs) {
    return lhs.getMemory() > rhs.getMemory();
  }

  public static boolean greaterThanOrEqual(Resource lhs, Resource rhs) {
    return lhs.getMemory() >= rhs.getMemory();
  }
}
