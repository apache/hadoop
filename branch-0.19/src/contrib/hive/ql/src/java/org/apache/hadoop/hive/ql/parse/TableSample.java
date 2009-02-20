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

public class TableSample {
  private int numerator;
  private int denominator;
  private ArrayList<String> cols;
  
  public TableSample(String num, String den, ArrayList<String> cols) {
    this.numerator = Integer.valueOf(num).intValue();
    this.denominator = Integer.valueOf(den).intValue();
    this.cols = cols;
  }
  public int getNumerator() {
    return this.numerator;
  }
  public void setNumerator(int num) {
    this.numerator = num;
  }
  public int getDenominator() {
    return this.denominator;
  }
  public void setDenominator(int den) {
    this.denominator = den;
  }
  public ArrayList<String> getCols() {
    return this.cols;
  }
  public void setCols(ArrayList<String> cols) {
    this.cols = cols;
  }

}
