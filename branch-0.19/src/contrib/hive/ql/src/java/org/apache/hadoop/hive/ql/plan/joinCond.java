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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * Join conditions Descriptor implementation.
 * 
 */
public class joinCond implements Serializable {
  private static final long serialVersionUID = 1L;
  private int left;
  private int right;
  private int type;

  public joinCond() {}

  public joinCond(int left, int right, int type) {
    this.left  = left;
    this.right = right;
    this.type  = type;
  }

  public joinCond(org.apache.hadoop.hive.ql.parse.joinCond condn) {
    this.left     = condn.getLeft();
    this.right    = condn.getRight();
    org.apache.hadoop.hive.ql.parse.joinType itype = condn.getJoinType();
    if (itype == org.apache.hadoop.hive.ql.parse.joinType.INNER)
      this.type = joinDesc.INNER_JOIN;
    else if (itype == org.apache.hadoop.hive.ql.parse.joinType.LEFTOUTER)
      this.type = joinDesc.LEFT_OUTER_JOIN;
    else if (itype == org.apache.hadoop.hive.ql.parse.joinType.RIGHTOUTER)
      this.type = joinDesc.RIGHT_OUTER_JOIN;
    else if (itype == org.apache.hadoop.hive.ql.parse.joinType.FULLOUTER)
      this.type = joinDesc.FULL_OUTER_JOIN;
    else
      assert false;
  }
  
  public int getLeft() {
    return this.left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return this.right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public int getType() {
    return this.type;
  }

  public void setType(final int type) {
    this.type = type;
  }
  
  @explain
  public String getJoinCondString() {
    StringBuilder sb = new StringBuilder();
    
    switch(type) {
    case joinDesc.INNER_JOIN:
      sb.append("Inner Join ");
      break;
    case joinDesc.FULL_OUTER_JOIN:
      sb.append("Outer Join ");
      break;
    case joinDesc.LEFT_OUTER_JOIN:
      sb.append("Left Outer Join");
      break;
    case joinDesc.RIGHT_OUTER_JOIN:
      sb.append("Right Outer Join");
      break;
    default:
      sb.append("Unknow Join");
      break;
    }
    
    sb.append(left);
    sb.append(" to ");
    sb.append(right);
    
    return sb.toString();
  }
}
