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
package org.apache.hadoop.fs.permission;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Defines a single entry in an ACL.  An ACL entry has a type (user, group,
 * mask, or other), an optional name (referring to a specific user or group), a
 * set of permissions (any combination of read, write and execute), and a scope
 * (access or default).  The natural ordering for entries within an ACL is:
 * <ol>
 * <li>owner entry (unnamed user)</li>
 * <li>all named user entries (internal ordering undefined)</li>
 * <li>owning group entry (unnamed group)</li>
 * <li>all named group entries (internal ordering undefined)</li>
 * <li>other entry</li>
 * </ol>
 * All access ACL entries sort ahead of all default ACL entries.  AclEntry
 * instances are immutable.  Use a {@link Builder} to create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AclEntry implements Comparable<AclEntry> {
  private final AclEntryType type;
  private final String name;
  private final FsAction permission;
  private final AclEntryScope scope;

  /**
   * Returns the ACL entry type.
   *
   * @return AclEntryType ACL entry type
   */
  public AclEntryType getType() {
    return type;
  }

  /**
   * Returns the optional ACL entry name.
   *
   * @return String ACL entry name, or null if undefined
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the set of permissions in the ACL entry.
   *
   * @return FsAction set of permissions in the ACL entry
   */
  public FsAction getPermission() {
    return permission;
  }

  /**
   * Returns the scope of the ACL entry.
   *
   * @return AclEntryScope scope of the ACL entry
   */
  public AclEntryScope getScope() {
    return scope;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    AclEntry other = (AclEntry)o;
    return Objects.equal(type, other.type) &&
      Objects.equal(name, other.name) &&
      Objects.equal(permission, other.permission) &&
      Objects.equal(scope, other.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, name, permission, scope);
  }

  @Override
  public int compareTo(AclEntry other) {
    return ComparisonChain.start()
      .compare(scope, other.scope, Ordering.explicit(ACCESS, DEFAULT))
      .compare(type, other.type, Ordering.explicit(USER, GROUP, MASK, OTHER))
      .compare(name, other.name, Ordering.natural().nullsFirst())
      .result();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (scope == AclEntryScope.DEFAULT) {
      sb.append("default:");
    }
    if (type != null) {
      sb.append(type.toString().toLowerCase());
    }
    sb.append(':');
    if (name != null) {
      sb.append(name);
    }
    sb.append(':');
    if (permission != null) {
      sb.append(permission.SYMBOL);
    }
    return sb.toString();
  }

  /**
   * Builder for creating new AclEntry instances.
   */
  public static class Builder {
    private AclEntryType type;
    private String name;
    private FsAction permission;
    private AclEntryScope scope = AclEntryScope.ACCESS;

    /**
     * Sets the ACL entry type.
     *
     * @param type AclEntryType ACL entry type
     * @return Builder this builder, for call chaining
     */
    public Builder setType(AclEntryType type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the optional ACL entry name.
     *
     * @param name String optional ACL entry name
     * @return Builder this builder, for call chaining
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the set of permissions in the ACL entry.
     *
     * @param permission FsAction set of permissions in the ACL entry
     * @return Builder this builder, for call chaining
     */
    public Builder setPermission(FsAction permission) {
      this.permission = permission;
      return this;
    }

    /**
     * Sets the scope of the ACL entry.  If this method is not called, then the
     * builder assumes {@link AclEntryScope#ACCESS}.
     *
     * @param scope AclEntryScope scope of the ACL entry
     * @return Builder this builder, for call chaining
     */
    public Builder setScope(AclEntryScope scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Builds a new AclEntry populated with the set properties.
     *
     * @return AclEntry new AclEntry
     */
    public AclEntry build() {
      return new AclEntry(type, name, permission, scope);
    }
  }

  /**
   * Private constructor.
   *
   * @param type AclEntryType ACL entry type
   * @param name String optional ACL entry name
   * @param permission FsAction set of permissions in the ACL entry
   * @param scope AclEntryScope scope of the ACL entry
   */
  private AclEntry(AclEntryType type, String name, FsAction permission, AclEntryScope scope) {
    this.type = type;
    this.name = name;
    this.permission = permission;
    this.scope = scope;
  }
}
