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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable

/**
 * Spill文件索引 在相应Reducer的数据请求时快速定位到相应的partition。
 * 一个Spill文件对应一个索引，索引存储专门分配的缓冲中（对应map输出的
 * 环形Buffer）
 */
public class IndexRecord {
  public long startOffset;
  public long rawLength;
  public long partLength;

  public IndexRecord() { }

  public IndexRecord(long startOffset, long rawLength, long partLength) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
  }
}
