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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;

/**
 * Interface for any S3A DT Service.
 */
public interface DTService extends Service {

  /**
   * Bind to the filesystem.
   * <p></p>
   * Subclasses can use this to perform their own binding operations -
   * but they must always call their superclass implementation.
   * This <i>Must</i> be called before calling {@code init()}.
   * <p></p>
   * <b>Important:</b>
   * This binding will happen during FileSystem.initialize(); the FS
   * is not live for actual use and will not yet have interacted with
   * AWS services.
   * @param uri the canonical URI of the FS.
   * @param context store context
   * @param delegationOperations delegation operations
   * @throws IOException failure.
   */
  void bindToFileSystem(
      URI uri,
      StoreContext context,
      DelegationOperations delegationOperations) throws IOException;

   /**
   * Bind to the filesystem.
   * <p></p>
   * Subclasses can use this to perform their own binding operations -
   * but they must always call their superclass implementation.
   * This <i>Must</i> be called before calling {@code init()}.
   * <p></p>
   * <b>Important:</b>
   * This binding will happen during FileSystem.initialize(); the FS
   * is not live for actual use and will not yet have interacted with
   * AWS services.
   * @param binding binding data
   * @throws IOException failure.
   */
  void initializeTokenBinding(ExtensionBindingData binding)
      throws IOException;

  /**
   * Get the canonical URI of the filesystem, which is what is
   * used to identify the tokens.
   * @return the URI.
   */
  URI getCanonicalUri();

  /**
   * Get the owner of this Service.
   * @return owner; non-null after binding to an FS.
   */
  UserGroupInformation getOwner();
}
