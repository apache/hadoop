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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * An AclStatus contains the ACL information of a specific file. AclStatus
 * instances are immutable. Use a {@link Builder} to create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AclStatus {
  private final String owner;
  private final String group;
  private final boolean stickyBit;
  private final List<AclEntry> entries;

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
   * Returns the sticky bit.
   * 
   * @return boolean sticky bit
   */
  public boolean isStickyBit() {
    return stickyBit;
  }

  /**
   * Returns the list of all ACL entries, ordered by their natural ordering.
   *
   * @return List<AclEntry> unmodifiable ordered list of all ACL entries
   */
  public List<AclEntry> getEntries() {
    return entries;
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
    return Objects.equal(owner, other.owner)
        && Objects.equal(group, other.group)
        && stickyBit == other.stickyBit
        && Objects.equal(entries, other.entries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(owner, group, stickyBit, entries);
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("owner: ").append(owner)
      .append(", group: ").append(group)
      .append(", acl: {")
      .append("entries: ").append(entries)
      .append(", stickyBit: ").append(stickyBit)
      .append('}')
      .toString();
  }

  /**
   * Builder for creating new Acl instances.
   */
  public static class Builder {
    private String owner;
    private String group;
    private boolean stickyBit;
    private List<AclEntry> entries = Lists.newArrayList();

    /**
     * Sets the file owner.
     *
     * @param owner String file owner
     * @return Builder this builder, for call chaining
     */
    public Builder owner(String owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Sets the file group.
     *
     * @param group String file group
     * @return Builder this builder, for call chaining
     */
    public Builder group(String group) {
      this.group = group;
      return this;
    }

    /**
     * Adds an ACL entry.
     *
     * @param e AclEntry entry to add
     * @return Builder this builder, for call chaining
     */
    public Builder addEntry(AclEntry e) {
      this.entries.add(e);
      return this;
    }

    /**
     * Adds a list of ACL entries.
     *
     * @param entries AclEntry entries to add
     * @return Builder this builder, for call chaining
     */
    public Builder addEntries(Iterable<AclEntry> entries) {
      for (AclEntry e : entries)
        this.entries.add(e);
      return this;
    }

    /**
     * Sets sticky bit. If this method is not called, then the builder assumes
     * false.
     *
     * @param stickyBit
     *          boolean sticky bit
     * @return Builder this builder, for call chaining
     */
    public Builder stickyBit(boolean stickyBit) {
      this.stickyBit = stickyBit;
      return this;
    }

    /**
     * Builds a new AclStatus populated with the set properties.
     *
     * @return AclStatus new AclStatus
     */
    public AclStatus build() {
      return new AclStatus(owner, group, stickyBit, entries);
    }
  }

  /**
   * Private constructor.
   *
   * @param file Path file associated to this ACL
   * @param owner String file owner
   * @param group String file group
   * @param stickyBit the sticky bit
   * @param entries the ACL entries
   */
  private AclStatus(String owner, String group, boolean stickyBit,
      Iterable<AclEntry> entries) {
    this.owner = owner;
    this.group = group;
    this.stickyBit = stickyBit;
    this.entries = Lists.newArrayList(entries);
  }
}
