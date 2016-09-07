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

import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class RawParser extends PermissionParser {
  private static Pattern rawOctalPattern =
      Pattern.compile("^\\s*([01]?)([0-7]{3})\\s*$");
  private static Pattern rawNormalPattern =
      Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxt]*)([,\\s]*)\\s*");

  private short permission;

  public RawParser(String modeStr) throws IllegalArgumentException {
    super(modeStr, rawNormalPattern, rawOctalPattern);
    permission = (short)combineModes(0, false);
  }

  public short getPermission() {
    return permission;
  }

}
