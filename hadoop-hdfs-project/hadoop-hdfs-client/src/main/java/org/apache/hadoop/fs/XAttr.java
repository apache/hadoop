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
package org.apache.hadoop.fs;

import java.util.Arrays;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * XAttr is the POSIX Extended Attribute model similar to that found in
 * traditional Operating Systems.  Extended Attributes consist of one
 * or more name/value pairs associated with a file or directory. Five
 * namespaces are defined: user, trusted, security, system and raw.
 *   1) USER namespace attributes may be used by any user to store
 *   arbitrary information. Access permissions in this namespace are
 *   defined by a file directory's permission bits. For sticky directories,
 *   only the owner and privileged user can write attributes.
 * <br>
 *   2) TRUSTED namespace attributes are only visible and accessible to
 *   privileged users. This namespace is available from both user space
 *   (filesystem API) and fs kernel.
 * <br>
 *   3) SYSTEM namespace attributes are used by the fs kernel to store
 *   system objects.  This namespace is only available in the fs
 *   kernel. It is not visible to users.
 * <br>
 *   4) SECURITY namespace attributes are used by the fs kernel for
 *   security features. It is not visible to users.
 * <br>
 *   5) RAW namespace attributes are used for internal system attributes that
 *   sometimes need to be exposed. Like SYSTEM namespace attributes they are
 *   not visible to the user except when getXAttr/getXAttrs is called on a file
 *   or directory in the /.reserved/raw HDFS directory hierarchy.  These
 *   attributes can only be accessed by the superuser.
 * <p/>
 * @see <a href="http://en.wikipedia.org/wiki/Extended_file_attributes">
 * http://en.wikipedia.org/wiki/Extended_file_attributes</a>
 *
 */
@InterfaceAudience.Private
public class XAttr {

  public enum NameSpace {
    USER,
    TRUSTED,
    SECURITY,
    SYSTEM,
    RAW
  }

  private final NameSpace ns;
  private final String name;
  private final byte[] value;

  public static class Builder {
    private NameSpace ns = NameSpace.USER;
    private String name;
    private byte[] value;

    public Builder setNameSpace(NameSpace ns) {
      this.ns = ns;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setValue(byte[] value) {
      this.value = value;
      return this;
    }

    public XAttr build() {
      return new XAttr(ns, name, value);
    }
  }

  private XAttr(NameSpace ns, String name, byte[] value) {
    this.ns = ns;
    this.name = name;
    this.value = value;
  }

  public NameSpace getNameSpace() {
    return ns;
  }

  public String getName() {
    return name;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(811, 67)
        .append(name)
        .append(ns)
        .append(value)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(ns, rhs.ns)
        .append(name, rhs.name)
        .append(value, rhs.value)
        .isEquals();
  }

  /**
   * Similar to {@link #equals(Object)}, except ignores the XAttr value.
   *
   * @param obj to compare equality
   * @return if the XAttrs are equal, ignoring the XAttr value
   */
  public boolean equalsIgnoreValue(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(ns, rhs.ns)
        .append(name, rhs.name)
        .isEquals();
  }

  @Override
  public String toString() {
    return "XAttr [ns=" + ns + ", name=" + name + ", value="
        + Arrays.toString(value) + "]";
  }
}
