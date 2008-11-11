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

import java.util.ArrayList;
import org.antlr.runtime.tree.CommonTree;

/**
 * 
 * This class stores all the information specified in the TABLESAMPLE clause. e.g. 
 * for the clause "FROM t TABLESAMPLE(1 OUT OF 2 ON c1) it will store the numerator
 * 1, the denominator 2 and the list of expressions(in this case c1) in the appropriate
 * fields. The afore-mentioned sampling clause causes the 1st bucket to be picked out of
 * the 2 buckets created by hashing on c1.
 *
 */
public class TableSample {
	
  /**
   * The numerator of the TABLESAMPLE clause
   */
  private int numerator;
  
  /**
   * The denominator of the TABLESAMPLE clause
   */
  private int denominator;
  
  /**
   * The list of expressions following ON part of the TABLESAMPLE clause. This list is
   * empty in case there are no expressions such as in the clause
   * "FROM t TABLESAMPLE(1 OUT OF 2)". For this expression the sampling is done
   * on the tables clustering column(as specified when the table was created). In case
   * the table does not have any clustering column, the usage of a table sample clause
   * without an ON part is disallowed by the compiler
   */
  private ArrayList<CommonTree> exprs;
  
  /**
   * Flag to indicate that input files can be pruned
   */
  private boolean inputPruning;
  
  /**
   * Constructs the TableSample given the numerator, denominator and the list of
   * ON clause expressions
   * 
   * @param num The numerator
   * @param den The denominator
   * @param exprs The list of expressions in the ON part of the TABLESAMPLE clause
   */
  public TableSample(String num, String den, ArrayList<CommonTree> exprs) {
    this.numerator = Integer.valueOf(num).intValue();
    this.denominator = Integer.valueOf(den).intValue();
    this.exprs = exprs;
  }
  
  /**
   * Gets the numerator
   * 
   * @return int
   */
  public int getNumerator() {
    return this.numerator;
  }
  
  /**
   * Sets the numerator
   * 
   * @param num The numerator
   */
  public void setNumerator(int num) {
    this.numerator = num;
  }
  
  /**
   * Gets the denominator
   * 
   * @return int
   */
  public int getDenominator() {
    return this.denominator;
  }
  
  /**
   * Sets the denominator
   * 
   * @param den The denominator
   */
  public void setDenominator(int den) {
    this.denominator = den;
  }
  
  /**
   * Gets the ON part's expression list
   * 
   * @return ArrayList<CommonTree>
   */
  public ArrayList<CommonTree> getExprs() {
    return this.exprs;
  }
  
  /**
   * Sets the expression list
   * 
   * @param exprs The expression list
   */
  public void setExprs(ArrayList<CommonTree> exprs) {
    this.exprs = exprs;
  }

  /**
   * Gets the flag that indicates whether input pruning is possible
   * 
   * @return boolean
   */
  public boolean getInputPruning() {
	  return this.inputPruning;
  }
 
  /**
   * Sets the flag that indicates whether input pruning is possible or not
   * 
   * @param inputPruning true if input pruning is possible
   */
  public void setInputPruning(boolean inputPruning) {
	  this.inputPruning = inputPruning;
  }
}
