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

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum XAttrSetFlag {
  /**
   * Create a new xattr.
   * If the xattr exists already, exception will be thrown.
   */
  CREATE((short) 0x01),

  /**
   * Replace a existing xattr.
   * If the xattr does not exist, exception will be thrown.
   */
  REPLACE((short) 0x02);

  private final short flag;

  private XAttrSetFlag(short flag) {
    this.flag = flag;
  }

  short getFlag() {
    return flag;
  }

  public static void validate(String xAttrName, boolean xAttrExists,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    if (flag == null || flag.isEmpty()) {
      throw new HadoopIllegalArgumentException("A flag must be specified.");
    }

    if (xAttrExists) {
      if (!flag.contains(REPLACE)) {
        throw new IOException("XAttr: " + xAttrName +
            " already exists. The REPLACE flag must be specified.");
      }
    } else {
      if (!flag.contains(CREATE)) {
        throw new IOException("XAttr: " + xAttrName +
            " does not exist. The CREATE flag must be specified.");
      }
    }
  }
}
