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

package org.apache.hadoop.registry.client.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a manifestation of the Zookeeper restrictions about
 * what nodes may act as parents.
 *
 * Children are not allowed under ephemeral nodes. This is an aspect
 * of ZK which isn't directly exposed to the registry API. It may
 * surface if the registry is manipulated outside of the registry API.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class NoChildrenForEphemeralsException extends RegistryIOException {
  public NoChildrenForEphemeralsException(String path, Throwable cause) {
    super(path, cause);
  }

  public NoChildrenForEphemeralsException(String path, String error) {
    super(path, error);
  }

  public NoChildrenForEphemeralsException(String path,
      String error,
      Throwable cause) {
    super(path, error, cause);
  }
}
