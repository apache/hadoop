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

/**
 * Adapter classes for allowing V1 credential providers to be used with SDKV2.
 * This is the only package where use of aws v1 classes are permitted;
 * all instantiations of objects here must use reflection to probe for
 * availability or be prepared to catch exceptions which may be raised
 * if the v1 SDK isn't found on the classpath
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.adapter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;