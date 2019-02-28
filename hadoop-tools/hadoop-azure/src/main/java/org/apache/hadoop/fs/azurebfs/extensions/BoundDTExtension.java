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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * An optional extension for custom extensions, so as to support
 * tighter integration.
 *
 * This interface can be implemented by either of a
 * {@link CustomDelegationTokenManager} or a {@link CustomTokenProviderAdaptee}.
 *
 * In both cases, extra lifecycle operation will be invoked.
 *
 * <ol>
 *   <li>{@link #bind(URI, Configuration)} will
 *   be invoked after {@code initialize()}</li>
 *   <li>{@link Closeable#close()} will be invoked
 *   when the Filesystem instance is closed.</li>
 * </ol>
 *
 * The {@link #getCanonicalServiceName()} will be invoked on a Custom
 * DT provider when the filesystem is asked for a Canonical Service Name.
 *
 * The {@link #getUserAgentSuffix()} is invoked on a CustomTokenProviderAdaptee
 * as the filesystem is initialized; the User Agent Suffix which it returns
 * is included in the UA header used for the ABFS Client -and so logged
 * in the ABFS access logs.
 *
 * This allows for token providers to to provide extra information
 * about the caller for use in auditing requests.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public interface BoundDTExtension extends Closeable {

  /**
   * Bind the extension to the specific instance of ABFS.
   * This happens during the ABFS's own initialization logic; it is unlikely
   * to be completely instantiated at this point.
   * Therefore, while a reference may be cached, implementations MUST NOT
   * invoke methods on it.
   * @param fsURI URI of the filesystem.
   * @param conf configuration of this extension.
   * @throws IOException failure during binding.
   */
  void bind(URI fsURI, Configuration conf) throws IOException;

  /**
   * Get the canonical service name, which will be
   * returned by {@code FileSystem.getCanonicalServiceName()} and so used to
   * map the issued DT in credentials, including credential files collected
   * for job submission.
   *
   * If null is returned: fall back to the default filesystem logic.
   *
   * Only invoked on {@link CustomDelegationTokenManager} instances.
   * @return the service name to be returned by the filesystem.
   */
  default String getCanonicalServiceName() {
    return null;
  }

  /**
   * Get a suffix for the UserAgent suffix of HTTP requests, which
   * can be used to identify the principal making ABFS requests.
   * @return an empty string, or a key=value string to be added to the UA
   * header.
   */
  default String getUserAgentSuffix() {
    return "";
  }

}
