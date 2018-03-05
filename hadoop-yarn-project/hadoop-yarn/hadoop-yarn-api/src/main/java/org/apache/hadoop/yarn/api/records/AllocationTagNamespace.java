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

package org.apache.hadoop.yarn.api.records;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.exceptions.InvalidAllocationTagException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.NOT_SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_LABEL;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_ID;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.ALL;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.fromString;

/**
 * Class to describe the namespace of an allocation tag.
 * Each namespace can be evaluated against a set of applications.
 * After evaluation, the namespace should have an implicit set of
 * applications which defines its scope.
 */
public abstract class AllocationTagNamespace implements
    Evaluable<TargetApplications> {

  public final static String NAMESPACE_DELIMITER = "/";

  private AllocationTagNamespaceType nsType;
  // Namespace scope value will be delay binding by eval method.
  private Set<ApplicationId> nsScope;

  public AllocationTagNamespace(AllocationTagNamespaceType
      allocationTagNamespaceType) {
    this.nsType = allocationTagNamespaceType;
  }

  protected void setScopeIfNotNull(Set<ApplicationId> appIds) {
    if (appIds != null) {
      this.nsScope = appIds;
    }
  }

  /**
   * Get the type of the namespace.
   * @return namespace type.
   */
  public AllocationTagNamespaceType getNamespaceType() {
    return nsType;
  }

  /**
   * Get the scope of the namespace, in form of a set of applications.
   * Before calling this method, {@link #evaluate(TargetApplications)}
   * must be called in prior to ensure the scope is proper evaluated.
   *
   * @return a set of applications.
   */
  public Set<ApplicationId> getNamespaceScope() {
    if (this.nsScope == null) {
      throw new IllegalStateException("Invalid namespace scope,"
          + " it is not initialized. Evaluate must be called before"
          + " a namespace can be consumed.");
    }
    return this.nsScope;
  }

  @Override
  public abstract void evaluate(TargetApplications target)
      throws InvalidAllocationTagException;

  /**
   * @return true if the namespace is effective in all applications
   * in this cluster. Specifically the namespace prefix should be
   * "all".
   */
  public boolean isGlobal() {
    return AllocationTagNamespaceType.ALL.equals(getNamespaceType());
  }

  /**
   * @return true if the namespace is effective within a single application
   * by its application ID, the namespace prefix should be "app-id";
   * false otherwise.
   */
  public boolean isSingleInterApp() {
    return AllocationTagNamespaceType.APP_ID.equals(getNamespaceType());
  }

  /**
   * @return true if the namespace is effective to the application itself,
   * the namespace prefix should be "self"; false otherwise.
   */
  public boolean isIntraApp() {
    return AllocationTagNamespaceType.SELF.equals(getNamespaceType());
  }

  /**
   * @return true if the namespace is effective to all applications except
   * itself, the namespace prefix should be "not-self"; false otherwise.
   */
  public boolean isNotSelf() {
    return AllocationTagNamespaceType.NOT_SELF.equals(getNamespaceType());
  }

  /**
   * @return true if the namespace is effective to a group of applications
   * identified by a application label, the namespace prefix should be
   * "app-label"; false otherwise.
   */
  public boolean isAppLabel() {
    return AllocationTagNamespaceType.APP_LABEL.equals(getNamespaceType());
  }

  @Override
  public String toString() {
    return this.nsType.toString();
  }

  /**
   * Namespace within application itself.
   */
  public static class Self extends AllocationTagNamespace {

    public Self() {
      super(SELF);
    }

    @Override
    public void evaluate(TargetApplications target)
        throws InvalidAllocationTagException {
      if (target == null || target.getCurrentApplicationId() == null) {
        throw new InvalidAllocationTagException("Namespace Self must"
            + " be evaluated against a single application ID.");
      }
      ApplicationId applicationId = target.getCurrentApplicationId();
      setScopeIfNotNull(ImmutableSet.of(applicationId));
    }
  }

  /**
   * Namespace to all applications except itself.
   */
  public static class NotSelf extends AllocationTagNamespace {

    private ApplicationId applicationId;

    public NotSelf() {
      super(NOT_SELF);
    }

    /**
     * The scope of self namespace is to an application itself,
     * the application ID can be delay binding to the namespace.
     *
     * @param appId application ID.
     */
    public void setApplicationId(ApplicationId appId) {
      this.applicationId = appId;
    }

    public ApplicationId getApplicationId() {
      return this.applicationId;
    }

    @Override
    public void evaluate(TargetApplications target) {
      Set<ApplicationId> otherAppIds = target.getOtherApplicationIds();
      setScopeIfNotNull(otherAppIds);
    }
  }

  /**
   * Namespace to all applications in the cluster.
   */
  public static class All extends AllocationTagNamespace {

    public All() {
      super(ALL);
    }

    @Override
    public void evaluate(TargetApplications target) {
      Set<ApplicationId> allAppIds = target.getAllApplicationIds();
      setScopeIfNotNull(allAppIds);
    }
  }

  /**
   * Namespace to all applications in the cluster.
   */
  public static class AppLabel extends AllocationTagNamespace {

    public AppLabel() {
      super(APP_LABEL);
    }

    @Override
    public void evaluate(TargetApplications target) {
      // TODO Implement app-label namespace evaluation
    }
  }

  /**
   * Namespace defined by a certain application ID.
   */
  public static class AppID extends AllocationTagNamespace {

    private ApplicationId targetAppId;
    // app-id namespace requires an extra value of an application id.
    public AppID(ApplicationId applicationId) {
      super(APP_ID);
      this.targetAppId = applicationId;
    }

    @Override
    public void evaluate(TargetApplications target) {
      setScopeIfNotNull(ImmutableSet.of(targetAppId));
    }

    @Override
    public String toString() {
      return APP_ID.toString() + NAMESPACE_DELIMITER + this.targetAppId;
    }
  }

  /**
   * Parse namespace from a string. The string must be in legal format
   * defined by each {@link AllocationTagNamespaceType}.
   *
   * @param namespaceStr namespace string.
   * @return an instance of {@link AllocationTagNamespace}.
   * @throws InvalidAllocationTagException
   * if given string is not in valid format
   */
  public static AllocationTagNamespace parse(String namespaceStr)
      throws InvalidAllocationTagException {
    // Return the default namespace if no valid string is given.
    if (Strings.isNullOrEmpty(namespaceStr)) {
      return new Self();
    }

    // Normalize the input, escape additional chars.
    List<String> nsValues = normalize(namespaceStr);
    // The first string should be the prefix.
    String nsPrefix = nsValues.get(0);
    AllocationTagNamespaceType allocationTagNamespaceType =
        fromString(nsPrefix);
    switch (allocationTagNamespaceType) {
    case SELF:
      return new Self();
    case NOT_SELF:
      return new NotSelf();
    case ALL:
      return new All();
    case APP_ID:
      if (nsValues.size() != 2) {
        throw new InvalidAllocationTagException(
            "Missing the application ID in the namespace string: "
                + namespaceStr);
      }
      String appIDStr = nsValues.get(1);
      return parseAppID(appIDStr);
    case APP_LABEL:
      return new AppLabel();
    default:
      throw new InvalidAllocationTagException(
          "Invalid namespace string " + namespaceStr);
    }
  }

  private static AllocationTagNamespace parseAppID(String appIDStr)
      throws InvalidAllocationTagException {
    try {
      ApplicationId applicationId = ApplicationId.fromString(appIDStr);
      return new AppID(applicationId);
    } catch (IllegalArgumentException e) {
      throw new InvalidAllocationTagException(
          "Invalid application ID for "
              + APP_ID.getTypeKeyword() + ": " + appIDStr);
    }
  }

  /**
   * Valid given namespace string and parse it to a list of sub-strings
   * that can be consumed by the parser according to the type of the
   * namespace. Currently the size of return list should be either 1 or 2.
   * Extra slash is escaped during the normalization.
   *
   * @param namespaceStr namespace string.
   * @return a list of parsed strings.
   * @throws InvalidAllocationTagException
   * if namespace format is unexpected.
   */
  private static List<String> normalize(String namespaceStr)
      throws InvalidAllocationTagException {
    List<String> result = new ArrayList<>();
    if (namespaceStr == null) {
      return result;
    }

    String[] nsValues = namespaceStr.split(NAMESPACE_DELIMITER);
    for (String str : nsValues) {
      if (!Strings.isNullOrEmpty(str)) {
        result.add(str);
      }
    }

    // Currently we only allow 1 or 2 values for a namespace string
    if (result.size() == 0 || result.size() > 2) {
      throw new InvalidAllocationTagException("Invalid namespace string: "
          + namespaceStr + ", the syntax is <namespace_prefix> or"
          + " <namespace_prefix>/<namespace_value>");
    }

    return result;
  }
}
