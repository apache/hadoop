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
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;

/**
 * There are four types of extended attributes &lt;XAttr&gt; defined by the
 * following namespaces:
 * <br>
 * USER - extended user attributes: these can be assigned to files and
 * directories to store arbitrary additional information. The access
 * permissions for user attributes are defined by the file permission
 * bits. For sticky directories, only the owner and privileged user can 
 * write attributes.
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
 * <br>
 * RAW - extended system attributes: these are used for internal system
 *   attributes that sometimes need to be exposed. Like SYSTEM namespace
 *   attributes they are not visible to the user except when getXAttr/getXAttrs
 *   is called on a file or directory in the /.reserved/raw HDFS directory
 *   hierarchy. These attributes can only be accessed by the user who have
 *   read access.
 * <br>
 */
@InterfaceAudience.Private
public class XAttrPermissionFilter {
  
  static void checkPermissionForApi(FSPermissionChecker pc, XAttr xAttr,
      boolean isRawPath)
      throws AccessControlException {
    final boolean isSuperUser = pc.isSuperUser();
    final String xAttrString =
        "XAttr [ns=" + xAttr.getNameSpace() + ", name=" + xAttr.getName() + "]";
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER || 
        (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && isSuperUser)) {
      if (isSuperUser) {
        // call the external enforcer for audit.
        pc.checkSuperuserPrivilege(xAttrString);
      }
      return;
    }
    if (xAttr.getNameSpace() == XAttr.NameSpace.RAW && isRawPath) {
      return;
    }
    if (XAttrHelper.getPrefixedName(xAttr).
        equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
      if (xAttr.getValue() != null) {
        // Notify external enforcer for audit
        String errorMessage = "Attempt to set a value for '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER +
            "'. Values are not allowed for this xattr.";
        pc.denyUserAccess(xAttrString, errorMessage);
      }
      return;
    }
    pc.denyUserAccess(xAttrString, "User doesn't have permission for xattr: "
            + XAttrHelper.getPrefixedName(xAttr));
  }

  static void checkPermissionForApi(FSPermissionChecker pc,
      List<XAttr> xAttrs, boolean isRawPath) throws AccessControlException {
    Preconditions.checkArgument(xAttrs != null);
    if (xAttrs.isEmpty()) {
      return;
    }

    for (XAttr xAttr : xAttrs) {
      checkPermissionForApi(pc, xAttr, isRawPath);
    }
  }

  static List<XAttr> filterXAttrsForApi(FSPermissionChecker pc,
      List<XAttr> xAttrs, boolean isRawPath) {
    assert xAttrs != null : "xAttrs can not be null";
    if (xAttrs.isEmpty()) {
      return xAttrs;
    }
    
    List<XAttr> filteredXAttrs = Lists.newArrayListWithCapacity(xAttrs.size());
    final boolean isSuperUser = pc.isSuperUser();
    for (XAttr xAttr : xAttrs) {
      if (xAttr.getNameSpace() == XAttr.NameSpace.USER) {
        filteredXAttrs.add(xAttr);
      } else if (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && 
          isSuperUser) {
        filteredXAttrs.add(xAttr);
      } else if (xAttr.getNameSpace() == XAttr.NameSpace.RAW && isRawPath) {
        filteredXAttrs.add(xAttr);
      } else if (XAttrHelper.getPrefixedName(xAttr).
          equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
        filteredXAttrs.add(xAttr);
      }
    }
    return filteredXAttrs;
  }
}
