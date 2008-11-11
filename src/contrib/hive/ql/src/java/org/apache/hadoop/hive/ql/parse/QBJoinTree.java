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

import java.util.Vector;

import org.antlr.runtime.tree.CommonTree;

/**
 * Internal representation of the join tree
 *
 */
public class QBJoinTree 
{
  private String        leftAlias;
  private String[]      rightAliases;
  private String[]      leftAliases;
  private QBJoinTree    joinSrc;
  private String[]      baseSrc;
  private int           nextTag;
  private joinCond[]    joinCond;
  private boolean       noOuterJoin;
  
  // join conditions
  private Vector<Vector<CommonTree>> expressions;

  // filters
  private Vector<Vector<CommonTree>> filters;
  
  /**
   * constructor 
   */
  public QBJoinTree() { nextTag = 0;}

  /**
   * returns left alias if any - this is used for merging later on
   * @return left alias if any
   */
  public String getLeftAlias() {
    return leftAlias;
  }

  /**
   * set left alias for the join expression
   * @param leftAlias String
   */
  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public Vector<Vector<CommonTree>> getExpressions() {
    return expressions;
  }

  public void setExpressions(Vector<Vector<CommonTree>> expressions) {
    this.expressions = expressions;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public QBJoinTree getJoinSrc() {
    return joinSrc;
  }

  public void setJoinSrc(QBJoinTree joinSrc) {
    this.joinSrc = joinSrc;
  }

  public int getNextTag() {
    return nextTag++;
  }

  public String getJoinStreamDesc() {
    return "$INTNAME";
  }

  public joinCond[] getJoinCond() {
    return joinCond;
  }

  public void setJoinCond(joinCond[] joinCond) {
    this.joinCond = joinCond;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

	/**
	 * @return the filters
	 */
	public Vector<Vector<CommonTree>> getFilters() {
		return filters;
	}

	/**
	 * @param filters the filters to set
	 */
	public void setFilters(Vector<Vector<CommonTree>> filters) {
		this.filters = filters;
	}

}


