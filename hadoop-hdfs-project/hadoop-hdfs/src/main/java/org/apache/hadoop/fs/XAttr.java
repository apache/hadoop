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

/**
 * XAttr is POSIX Extended Attribute model, similar to the one in traditional 
 * Operating Systems. Extended Attribute consists of a name and associated 
 * data, and 4 namespaces are defined: user, trusted, security and system.
 * <br>
 *   1). USER namespace extended attribute may be assigned for storing 
 *   arbitrary additional information, and its access permissions are 
 *   defined by file/directory permission bits.
 *   <br>
 *   2). TRUSTED namespace extended attribute are visible and accessible 
 *   only to privilege user (file/directory owner or fs admin), and it is 
 *   available from both user space (filesystem API) and fs kernel.
 *   <br>
 *   3). SYSTEM namespace extended attribute is used by fs kernel to store 
 *   system objects, and only available in fs kernel. It's not visible to users.
 *   <br>
 *   4). SECURITY namespace extended attribute is used by fs kernel for 
 *   security features, and it's not visible to users.
 * <p/>
 * @see <a href="http://en.wikipedia.org/wiki/Extended_file_attributes">
 * http://en.wikipedia.org/wiki/Extended_file_attributes</a>
 *
 */
public class XAttr {

  public static enum NameSpace {
    USER,
    TRUSTED,
    SECURITY,
    SYSTEM;
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
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((ns == null) ? 0 : ns.hashCode());
    result = prime * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    XAttr other = (XAttr) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (ns != other.ns) {
      return false;
    }
    if (!Arrays.equals(value, other.value)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    return "XAttr [ns=" + ns + ", name=" + name + ", value="
        + Arrays.toString(value) + "]";
  }
}
