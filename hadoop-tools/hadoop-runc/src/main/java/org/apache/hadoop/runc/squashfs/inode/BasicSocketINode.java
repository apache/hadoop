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

package org.apache.hadoop.runc.squashfs.inode;

public class BasicSocketINode extends AbstractBasicIpcINode
    implements SocketINode {

  static SocketINode simplify(SocketINode src) {
    if (src instanceof BasicSocketINode) {
      return src;
    }

    if (src.isXattrPresent()) {
      return src;
    }

    BasicSocketINode dest = new BasicSocketINode();
    src.copyTo(dest);

    dest.setNlink(src.getNlink());
    return dest;
  }

  @Override
  protected String getName() {
    return "basic-socket-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.BASIC_SOCKET;
  }

  @Override
  public SocketINode simplify() {
    return this;
  }

}
