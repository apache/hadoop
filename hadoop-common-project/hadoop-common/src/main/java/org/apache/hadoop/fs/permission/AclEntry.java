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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines a single entry in an ACL.  An ACL entry has a type (user, group,
 * mask, or other), an optional name (referring to a specific user or group), a
 * set of permissions (any combination of read, write and execute), and a scope
 * (access or default).  AclEntry instances are immutable.  Use a {@link Builder}
 * to create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AclEntry {
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
  @InterfaceStability.Unstable
  public String toString() {
    // This currently just delegates to the stable string representation, but it
    // is permissible for the output of this method to change across versions.
    return toStringStable();
  }

  /**
   * Returns a string representation guaranteed to be stable across versions to
   * satisfy backward compatibility requirements, such as for shell command
   * output or serialization.  The format of this string representation matches
   * what is expected by the {@link #parseAclSpec(String, boolean)} and
   * {@link #parseAclEntry(String, boolean)} methods.
   *
   * @return stable, backward compatible string representation
   */
  public String toStringStable() {
    StringBuilder sb = new StringBuilder();
    if (scope == AclEntryScope.DEFAULT) {
      sb.append("default:");
    }
    if (type != null) {
      sb.append(StringUtils.toLowerCase(type.toStringStable()));
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
      if (name != null && !name.isEmpty()) {
        this.name = name;
      }
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

  /**
   * Parses a string representation of an ACL spec into a list of AclEntry
   * objects. Example: "user::rwx,user:foo:rw-,group::r--,other::---"
   * The expected format of ACL entries in the string parameter is the same
   * format produced by the {@link #toStringStable()} method.
   * 
   * @param aclSpec
   *          String representation of an ACL spec.
   * @param includePermission
   *          for setAcl operations this will be true. i.e. AclSpec should
   *          include permissions.<br>
   *          But for removeAcl operation it will be false. i.e. AclSpec should
   *          not contain permissions.<br>
   *          Example: "user:foo,group:bar"
   * @return Returns list of {@link AclEntry} parsed
   */
  public static List<AclEntry> parseAclSpec(String aclSpec,
      boolean includePermission) {
    List<AclEntry> aclEntries = new ArrayList<AclEntry>();
    Collection<String> aclStrings = StringUtils.getStringCollection(aclSpec,
        ",");
    for (String aclStr : aclStrings) {
      AclEntry aclEntry = parseAclEntry(aclStr, includePermission);
      aclEntries.add(aclEntry);
    }
    return aclEntries;
  }

  /**
   * Parses a string representation of an ACL into a AclEntry object.<br>
   * The expected format of ACL entries in the string parameter is the same
   * format produced by the {@link #toStringStable()} method.
   * 
   * @param aclStr
   *          String representation of an ACL.<br>
   *          Example: "user:foo:rw-"
   * @param includePermission
   *          for setAcl operations this will be true. i.e. Acl should include
   *          permissions.<br>
   *          But for removeAcl operation it will be false. i.e. Acl should not
   *          contain permissions.<br>
   *          Example: "user:foo,group:bar,mask::"
   * @return Returns an {@link AclEntry} object
   */
  public static AclEntry parseAclEntry(String aclStr,
      boolean includePermission) {
    AclEntry.Builder builder = new AclEntry.Builder();
    // Here "::" represent one empty string.
    // StringUtils.getStringCollection() will ignore this.
    String[] split = aclStr.split(":");

    if (split.length == 0) {
      throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
    }
    int index = 0;
    if ("default".equals(split[0])) {
      // default entry
      index++;
      builder.setScope(AclEntryScope.DEFAULT);
    }

    if (split.length <= index) {
      throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
    }

    AclEntryType aclType = null;
    try {
      aclType = Enum.valueOf(
          AclEntryType.class, StringUtils.toUpperCase(split[index]));
      builder.setType(aclType);
      index++;
    } catch (IllegalArgumentException iae) {
      throw new HadoopIllegalArgumentException(
          "Invalid type of acl in <aclSpec> :" + aclStr);
    }

    if (split.length > index) {
      String name = split[index];
      if (!name.isEmpty()) {
        builder.setName(name);
      }
      index++;
    }

    if (includePermission) {
      if (split.length <= index) {
        throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "
            + aclStr);
      }
      String permission = split[index];
      FsAction fsAction = FsAction.getFsAction(permission);
      if (null == fsAction) {
        throw new HadoopIllegalArgumentException(
            "Invalid permission in <aclSpec> : " + aclStr);
      }
      builder.setPermission(fsAction);
      index++;
    }

    if (split.length > index) {
      throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
    }
    AclEntry aclEntry = builder.build();
    return aclEntry;
  }

  /**
   * Convert a List of AclEntries into a string - the reverse of parseAclSpec.
   * @param aclSpec List of AclEntries to convert
   * @return String representation of aclSpec
   */
  public static String aclSpecToString(List<AclEntry> aclSpec) {
    StringBuilder buf = new StringBuilder();
    for ( AclEntry e : aclSpec ) {
      buf.append(e.toString());
      buf.append(",");
    }
    return buf.substring(0, buf.length()-1);  // remove last ,
  }
}
