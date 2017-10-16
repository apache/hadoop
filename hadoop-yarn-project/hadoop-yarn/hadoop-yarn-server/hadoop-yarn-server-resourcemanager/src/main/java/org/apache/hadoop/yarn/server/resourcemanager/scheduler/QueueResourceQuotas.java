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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * QueueResourceQuotas by Labels for following fields by label
 * - EFFECTIVE_MIN_CAPACITY
 * - EFFECTIVE_MAX_CAPACITY
 * This class can be used to track resource usage in queue/user/app.
 *
 * And it is thread-safe
 */
public class QueueResourceQuotas extends AbstractResourceUsage {
  // short for no-label :)
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  public QueueResourceQuotas() {
    super();
  }

  /*
   * Configured Minimum Resource
   */
  public Resource getConfiguredMinResource() {
    return _get(NL, ResourceType.MIN_RESOURCE);
  }

  public Resource getConfiguredMinResource(String label) {
    return _get(label, ResourceType.MIN_RESOURCE);
  }

  public void setConfiguredMinResource(String label, Resource res) {
    _set(label, ResourceType.MIN_RESOURCE, res);
  }

  public void setConfiguredMinResource(Resource res) {
    _set(NL, ResourceType.MIN_RESOURCE, res);
  }

  /*
   * Configured Maximum Resource
   */
  public Resource getConfiguredMaxResource() {
    return getConfiguredMaxResource(NL);
  }

  public Resource getConfiguredMaxResource(String label) {
    return _get(label, ResourceType.MAX_RESOURCE);
  }

  public void setConfiguredMaxResource(Resource res) {
    setConfiguredMaxResource(NL, res);
  }

  public void setConfiguredMaxResource(String label, Resource res) {
    _set(label, ResourceType.MAX_RESOURCE, res);
  }

  /*
   * Effective Minimum Resource
   */
  public Resource getEffectiveMinResource() {
    return _get(NL, ResourceType.EFF_MIN_RESOURCE);
  }

  public Resource getEffectiveMinResource(String label) {
    return _get(label, ResourceType.EFF_MIN_RESOURCE);
  }

  public void setEffectiveMinResource(String label, Resource res) {
    _set(label, ResourceType.EFF_MIN_RESOURCE, res);
  }

  public void setEffectiveMinResource(Resource res) {
    _set(NL, ResourceType.EFF_MIN_RESOURCE, res);
  }

  /*
   * Effective Maximum Resource
   */
  public Resource getEffectiveMaxResource() {
    return getEffectiveMaxResource(NL);
  }

  public Resource getEffectiveMaxResource(String label) {
    return _get(label, ResourceType.EFF_MAX_RESOURCE);
  }

  public void setEffectiveMaxResource(Resource res) {
    setEffectiveMaxResource(NL, res);
  }

  public void setEffectiveMaxResource(String label, Resource res) {
    _set(label, ResourceType.EFF_MAX_RESOURCE, res);
  }
}
