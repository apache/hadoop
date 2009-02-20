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

package org.apache.hadoop.hive.ql.parse;

/**
 * Join conditions Descriptor implementation.
 * 
 */
public class joinCond {
  private int left;
  private int right;
  private joinType joinType;

  public joinCond() {  }

  public joinCond(int left, int right, joinType joinType) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
  }

  public int getLeft() {
    return left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public joinType getJoinType() {
    return joinType;
  }

  public void setJoinType(final joinType joinType) {
    this.joinType = joinType;
  }

}
