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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.security.AccessControlException;

import com.google.common.collect.Lists;

/**
 * There are four types of extended attributes <XAttr> defined by the
 * following namespaces:
 * <br>
 * USER - extended user attributes: these can be assigned to files and
 * directories to store arbitrary additional information. The access
 * permissions for user attributes are defined by the file permission
 * bits.
 * <br>
 * TRUSTED - trusted extended attributes: these are visible/accessible
 * only to/by the super user.
 * <br>
 * SECURITY - extended security attributes: these are used by the HDFS
 * core for security purposes and are not available through admin/user
 * API.
 * <br>
 * SYSTEM - extended system attributes: these are used by the HDFS
 * core and are not available through admin/user API.
 */
@InterfaceAudience.Private
public class XAttrPermissionFilter {
  
  static void checkPermissionForApi(FSPermissionChecker pc, XAttr xAttr) 
      throws AccessControlException {
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER || 
        (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && 
        pc.isSuperUser())) {
      return;
    }
    throw new AccessControlException("User doesn't have permission for xattr: "
        + XAttrHelper.getPrefixName(xAttr));
  }
  
  static List<XAttr> filterXAttrsForApi(FSPermissionChecker pc, 
      List<XAttr> xAttrs) {
    assert xAttrs != null : "xAttrs can not be null";
    if (xAttrs == null || xAttrs.isEmpty()) {
      return xAttrs;
    }
    
    List<XAttr> filteredXAttrs = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      if (xAttr.getNameSpace() == XAttr.NameSpace.USER) {
        filteredXAttrs.add(xAttr);
      } else if (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && 
          pc.isSuperUser()) {
        filteredXAttrs.add(xAttr);
      }
    }
    
    return filteredXAttrs;
  }
}
