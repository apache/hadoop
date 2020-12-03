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

public interface FileINode extends INode {

  int FRAGMENT_BLOCK_INDEX_NONE = 0xffff_ffff;

  long getBlocksStart();

  void setBlocksStart(long blocksStart);

  int getFragmentBlockIndex();

  void setFragmentBlockIndex(int fragmentBlockIndex);

  int getFragmentOffset();

  void setFragmentOffset(int fragmentOffset);

  long getFileSize();

  void setFileSize(long fileSize);

  int[] getBlockSizes();

  void setBlockSizes(int[] blockSizes);

  long getSparse();

  void setSparse(long sparse);

  int getNlink();

  void setNlink(int nlink);

  int getXattrIndex();

  void setXattrIndex(int xattrIndex);

  boolean isSparseBlockPresent();

  boolean isFragmentPresent();

  boolean isXattrPresent();

  FileINode simplify();

}
