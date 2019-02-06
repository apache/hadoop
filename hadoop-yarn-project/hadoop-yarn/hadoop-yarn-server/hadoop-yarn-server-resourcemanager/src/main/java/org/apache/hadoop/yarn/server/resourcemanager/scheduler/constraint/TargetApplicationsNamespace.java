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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.NOT_SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_TAG;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_ID;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.ALL;

/**
 * Class to describe the namespace of allocation tags, used by
 * {@link AllocationTags}. Each namespace can be evaluated against
 * a target set applications, represented by {@link TargetApplications}.
 * After evaluation, the namespace is interpreted to be a set of
 * applications based on the namespace type.
 */
public abstract class TargetApplicationsNamespace implements
    Evaluable<TargetApplications> {

  public final static String NAMESPACE_DELIMITER = "/";

  private AllocationTagNamespaceType nsType;
  // Namespace scope value will be delay binding by eval method.
  private Set<ApplicationId> nsScope;

  public TargetApplicationsNamespace(AllocationTagNamespaceType
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

  /**
   * Evaluate the namespace against given target applications
   * if it is necessary. Only self/not-self/app-label namespace types
   * require this evaluation step, because they are not binding to a
   * specific scope during initiating. So we do lazy binding for them
   * in this method.
   *
   * @param target a generic type target that impacts this evaluation.
   * @throws InvalidAllocationTagsQueryException
   */
  @Override
  public void evaluate(TargetApplications target)
      throws InvalidAllocationTagsQueryException {
    // Sub-class needs to override this when it requires the eval step.
  }

  @Override
  public String toString() {
    return this.nsType.toString();
  }

  /**
   * Namespace within application itself.
   */
  public static class Self extends TargetApplicationsNamespace {

    public Self() {
      super(SELF);
    }

    @Override
    public void evaluate(TargetApplications target)
        throws InvalidAllocationTagsQueryException {
      if (target == null || target.getCurrentApplicationId() == null) {
        throw new InvalidAllocationTagsQueryException("Namespace Self must"
            + " be evaluated against a single application ID.");
      }
      ApplicationId applicationId = target.getCurrentApplicationId();
      setScopeIfNotNull(ImmutableSet.of(applicationId));
    }
  }

  /**
   * Namespace to all applications except itself.
   */
  public static class NotSelf extends TargetApplicationsNamespace {

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
  public static class All extends TargetApplicationsNamespace {

    public All() {
      super(ALL);
    }
  }

  /**
   * Namespace to applications that attached with a certain application tag.
   */
  public static class AppTag extends TargetApplicationsNamespace {

    private String applicationTag;

    public AppTag(String appTag) {
      super(APP_TAG);
      this.applicationTag = appTag;
    }

    @Override
    public void evaluate(TargetApplications target) {
      setScopeIfNotNull(target.getApplicationIdsByTag(applicationTag));
    }

    @Override
    public String toString() {
      return APP_TAG.toString() + NAMESPACE_DELIMITER + this.applicationTag;
    }
  }

  /**
   * Namespace defined by a certain application ID.
   */
  public static class AppID extends TargetApplicationsNamespace {

    private ApplicationId targetAppId;
    // app-id namespace requires an extra value of an application id.
    public AppID(ApplicationId applicationId) {
      super(APP_ID);
      this.targetAppId = applicationId;
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
   * @return an instance of {@link TargetApplicationsNamespace}.
   * @throws InvalidAllocationTagsQueryException
   * if given string is not in valid format
   */
  public static TargetApplicationsNamespace parse(String namespaceStr)
      throws InvalidAllocationTagsQueryException {
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
        throw new InvalidAllocationTagsQueryException(
            "Missing the application ID in the namespace string: "
                + namespaceStr);
      }
      String appIDStr = nsValues.get(1);
      return parseAppID(appIDStr);
    case APP_TAG:
      if (nsValues.size() != 2) {
        throw new InvalidAllocationTagsQueryException(
            "Missing the application tag in the namespace string: "
                + namespaceStr);
      }
      return new AppTag(nsValues.get(1));
    default:
      throw new InvalidAllocationTagsQueryException(
          "Invalid namespace string " + namespaceStr);
    }
  }

  private static AllocationTagNamespaceType fromString(String prefix) throws
      InvalidAllocationTagsQueryException {
    for (AllocationTagNamespaceType type :
        AllocationTagNamespaceType.values()) {
      if(type.getTypeKeyword().equals(prefix)) {
        return type;
      }
    }

    Set<String> values = Arrays.stream(AllocationTagNamespaceType.values())
        .map(AllocationTagNamespaceType::toString)
        .collect(Collectors.toSet());
    throw new InvalidAllocationTagsQueryException(
        "Invalid namespace prefix: " + prefix
            + ", valid values are: " + String.join(",", values));
  }

  private static TargetApplicationsNamespace parseAppID(String appIDStr)
      throws InvalidAllocationTagsQueryException {
    try {
      ApplicationId applicationId = ApplicationId.fromString(appIDStr);
      return new AppID(applicationId);
    } catch (IllegalArgumentException e) {
      throw new InvalidAllocationTagsQueryException(
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
   * @throws InvalidAllocationTagsQueryException
   * if namespace format is unexpected.
   */
  private static List<String> normalize(String namespaceStr)
      throws InvalidAllocationTagsQueryException {
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
      throw new InvalidAllocationTagsQueryException("Invalid namespace string: "
          + namespaceStr + ", the syntax is <namespace_prefix> or"
          + " <namespace_prefix>/<namespace_value>");
    }

    return result;
  }
}
