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

import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * An AclStatus represents an association of a specific file {@link Path} with
 * an {@link Acl}.  AclStatus instances are immutable.  Use a {@link Builder} to
 * create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AclStatus {
  private final Path file;
  private final String owner;
  private final String group;
  private final Acl acl;

  /**
   * Returns the file associated to this ACL.
   *
   * @return Path file associated to this ACL
   */
  public Path getFile() {
    return file;
  }

  /**
   * Returns the file owner.
   *
   * @return String file owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns the file group.
   *
   * @return String file group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Returns the ACL.
   *
   * @return Acl the ACL
   */
  public Acl getAcl() {
    return acl;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    AclStatus other = (AclStatus)o;
    return Objects.equal(file, other.file) &&
      Objects.equal(owner, other.owner) &&
      Objects.equal(group, other.group) &&
      Objects.equal(acl, other.acl);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(file, owner, group, acl);
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("file: ").append(file)
      .append(", owner: ").append(owner)
      .append(", group: ").append(group)
      .append(", acl: {").append(acl).append('}')
      .toString();
  }

  /**
   * Builder for creating new Acl instances.
   */
  public static class Builder {
    private Path file;
    private String owner;
    private String group;
    private Acl acl;

    /**
     * Sets the file associated to this ACL.
     *
     * @param file Path file associated to this ACL
     * @return Builder this builder, for call chaining
     */
    public Builder setFile(Path file) {
      this.file = file;
      return this;
    }

    /**
     * Sets the file owner.
     *
     * @param owner String file owner
     * @return Builder this builder, for call chaining
     */
    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Sets the file group.
     *
     * @param group String file group
     * @return Builder this builder, for call chaining
     */
    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    /**
     * Sets the ACL.
     *
     * @param acl Acl the ACL
     * @return Builder this builder, for call chaining
     */
    public Builder setAcl(Acl acl) {
      this.acl = acl;
      return this;
    }

    /**
     * Builds a new Acl populated with the set properties.
     *
     * @return Acl new Acl
     */
    public AclStatus build() {
      return new AclStatus(file, owner, group, acl);
    }
  }

  /**
   * Private constructor.
   *
   * @param file Path file associated to this ACL
   * @param owner String file owner
   * @param group String file group
   * @param acl Acl the ACL
   */
  private AclStatus(Path file, String owner, String group, Acl acl) {
    this.file = file;
    this.owner = owner;
    this.group = group;
    this.acl = acl;
  }
}
