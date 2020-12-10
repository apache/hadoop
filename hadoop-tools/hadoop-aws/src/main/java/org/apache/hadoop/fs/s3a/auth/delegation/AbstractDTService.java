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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;

import static java.util.Objects.requireNonNull;

/**
 * This is the base class for both the delegation binding
 * code and the back end service created; allows for
 * shared methods across both.
 *
 * The lifecycle sequence is as follows
 * <pre>
 *   - create
 *   - bindToFileSystem(uri, ownerFS)
 *   - init
 *   - start
 *   ...api calls...
 *   - stop
 * </pre>
 *
 * As the S3ADelegation mechanism is all configured during the filesystem
 * initalize() operation, it is not ready for use through all the start process.
 */
public abstract class AbstractDTService
    extends AbstractService {

  /**
   * URI of the filesystem.
   * Valid after {@link #bindToFileSystem(URI, StoreContext, DelegationOperations)}.
   */
  private URI canonicalUri;

  /**
   * Owner of the filesystem.
   * Valid after {@link #bindToFileSystem(URI, StoreContext, DelegationOperations)}.
   */
  private UserGroupInformation owner;

  /**
   * Store Context for callbacks into the FS.
   * Valid after {@link #bindToFileSystem(URI, StoreContext, DelegationOperations)}.
   */
  private StoreContext storeContext;

  /**
   * Callbacks for DT-related operations.
   */
  private DelegationOperations policyProvider;

  /**
   * Protected constructor.
   * @param name service name.
   */
  protected AbstractDTService(final String name) {
    super(name);
  }

  /**
   * Bind to the filesystem.
   * Subclasses can use this to perform their own binding operations -
   * but they must always call their superclass implementation.
   * This <i>Must</i> be called before calling {@code init()}.
   *
   * <b>Important:</b>
   * This binding will happen during FileSystem.initialize(); the FS
   * is not live for actual use and will not yet have interacted with
   * AWS services.
   * @param uri the canonical URI of the FS.
   * @param context store context
   * @param delegationOperations delegation operations
   * @throws IOException failure.
   */
  public void bindToFileSystem(
      final URI uri,
      final StoreContext context,
      final DelegationOperations delegationOperations) throws IOException {
    requireServiceState(STATE.NOTINITED);
    Preconditions.checkState(canonicalUri == null,
        "bindToFileSystem called twice");
    this.canonicalUri = requireNonNull(uri);
    this.storeContext = requireNonNull(context);
    this.owner = context.getOwner();
    this.policyProvider = delegationOperations;
  }

  /**
   * Get the canonical URI of the filesystem, which is what is
   * used to identify the tokens.
   * @return the URI.
   */
  public URI getCanonicalUri() {
    return canonicalUri;
  }

  /**
   * Get the owner of this Service.
   * @return owner; non-null after binding to an FS.
   */
  public UserGroupInformation getOwner() {
    return owner;
  }

  protected StoreContext getStoreContext() {
    return storeContext;
  }

  protected DelegationOperations getPolicyProvider() {
    return policyProvider;
  }

  /**
   * Require that the service is in a given state.
   * @param state desired state.
   * @throws IllegalStateException if the condition is not met
   */
  protected void requireServiceState(final STATE state)
      throws IllegalStateException {
    Preconditions.checkState(isInState(state),
        "Required State: %s; Actual State %s", state, getServiceState());
  }

  /**
   * Require the service to be started.
   * @throws IllegalStateException if it is not.
   */
  protected void requireServiceStarted() throws IllegalStateException {
    requireServiceState(STATE.STARTED);
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    requireNonNull(canonicalUri, "service does not have a canonical URI");
  }
}
