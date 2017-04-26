/*
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

package org.apache.slider.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.slider.util.ServiceApiUtil.$;

/**
 * This class defines constants that can be used in input spec for
 * variable substitutions
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ServiceApiConstants {

  // Constants for service
  String SERVICE_NAME = $("SERVICE_NAME");

  String SERVICE_NAME_LC = $("SERVICE_NAME.lc");

  // Constants for component
  String COMPONENT_NAME = $("COMPONENT_NAME");

  String COMPONENT_NAME_LC = $("COMPONENT_NAME.lc");

  String COMPONENT_INSTANCE_NAME = $("COMPONENT_INSTANCE_NAME");

  // Constants for component instance
  String COMPONENT_ID = $("COMPONENT_ID");

  String CONTAINER_ID = $("CONTAINER_ID");
}
