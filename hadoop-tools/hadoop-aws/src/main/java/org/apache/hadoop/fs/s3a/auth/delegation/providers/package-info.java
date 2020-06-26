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
 * Package for implementations of S3A DT providers and identifiers.
 * <p></p>
 * New bindings should go here.
 * <p></p>
 * Note: although InterfaceAudience.Private at the java level, the classnames
 * of providers are referenced in configuration files,
 * and MUST NOT be changed.
 * <p></p>
 * When adding new bindings, remember to add their Identifiers to
 * {@code META-INF/services/org.apache.hadoop.security.token.TokenIdentifier}.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.auth.delegation.providers;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
