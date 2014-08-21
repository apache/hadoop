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
package org.apache.hadoop.hdfs;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttr.NameSpace;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@InterfaceAudience.Private
public class XAttrHelper {
  
  /**
   * Build <code>XAttr</code> from xattr name with prefix.
   */
  public static XAttr buildXAttr(String name) {
    return buildXAttr(name, null);
  }
  
  /**
   * Build <code>XAttr</code> from name with prefix and value.
   * Name can not be null. Value can be null. The name and prefix 
   * are validated.
   * Both name and namespace are case sensitive.
   */
  public static XAttr buildXAttr(String name, byte[] value) {
    Preconditions.checkNotNull(name, "XAttr name cannot be null.");
    
    final int prefixIndex = name.indexOf(".");
    if (prefixIndex < 3) {// Prefix length is at least 3.
      throw new HadoopIllegalArgumentException("An XAttr name must be " +
          "prefixed with user/trusted/security/system/raw, followed by a '.'");
    } else if (prefixIndex == name.length() - 1) {
      throw new HadoopIllegalArgumentException("XAttr name cannot be empty.");
    }
    
    NameSpace ns;
    final String prefix = name.substring(0, prefixIndex).toLowerCase();
    if (prefix.equals(NameSpace.USER.toString().toLowerCase())) {
      ns = NameSpace.USER;
    } else if (prefix.equals(NameSpace.TRUSTED.toString().toLowerCase())) {
      ns = NameSpace.TRUSTED;
    } else if (prefix.equals(NameSpace.SYSTEM.toString().toLowerCase())) {
      ns = NameSpace.SYSTEM;
    } else if (prefix.equals(NameSpace.SECURITY.toString().toLowerCase())) {
      ns = NameSpace.SECURITY;
    } else if (prefix.equals(NameSpace.RAW.toString().toLowerCase())) {
      ns = NameSpace.RAW;
    } else {
      throw new HadoopIllegalArgumentException("An XAttr name must be " +
          "prefixed with user/trusted/security/system/raw, followed by a '.'");
    }
    XAttr xAttr = (new XAttr.Builder()).setNameSpace(ns).setName(name.
        substring(prefixIndex + 1)).setValue(value).build();
    
    return xAttr;
  }
  
  /**
   * Build xattr name with prefix as <code>XAttr</code> list.
   */
  public static List<XAttr> buildXAttrAsList(String name) {
    XAttr xAttr = buildXAttr(name);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    
    return xAttrs;
  }
  
  /**
   * Get value of first xattr from <code>XAttr</code> list
   */
  public static byte[] getFirstXAttrValue(List<XAttr> xAttrs) {
    byte[] value = null;
    XAttr xAttr = getFirstXAttr(xAttrs);
    if (xAttr != null) {
      value = xAttr.getValue();
      if (value == null) {
        value = new byte[0]; // xattr exists, but no value.
      }
    }
    return value;
  }
  
  /**
   * Get first xattr from <code>XAttr</code> list
   */
  public static XAttr getFirstXAttr(List<XAttr> xAttrs) {
    if (xAttrs != null && !xAttrs.isEmpty()) {
      return xAttrs.get(0);
    }
    
    return null;
  }
  
  /**
   * Build xattr map from <code>XAttr</code> list, the key is 
   * xattr name with prefix, and value is xattr value. 
   */
  public static Map<String, byte[]> buildXAttrMap(List<XAttr> xAttrs) {
    if (xAttrs == null) {
      return null;
    }
    Map<String, byte[]> xAttrMap = Maps.newHashMap();
    for (XAttr xAttr : xAttrs) {
      String name = getPrefixName(xAttr);
      byte[] value = xAttr.getValue();
      if (value == null) {
        value = new byte[0];
      }
      xAttrMap.put(name, value);
    }
    
    return xAttrMap;
  }
  
  /**
   * Get name with prefix from <code>XAttr</code>
   */
  public static String getPrefixName(XAttr xAttr) {
    if (xAttr == null) {
      return null;
    }
    
    String namespace = xAttr.getNameSpace().toString();
    return namespace.toLowerCase() + "." + xAttr.getName();
  }

  /**
   * Build <code>XAttr</code> list from xattr name list.
   */
  public static List<XAttr> buildXAttrs(List<String> names) {
    if (names == null || names.isEmpty()) {
      throw new HadoopIllegalArgumentException("XAttr names can not be " +
          "null or empty.");
    }
    
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(names.size());
    for (String name : names) {
      xAttrs.add(buildXAttr(name, null));
    }
    return xAttrs;
  } 
}
