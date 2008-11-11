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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.fs.Path;

/**
 * 
 * This class stores the mapping from table alias to the parse tree information of the table
 * sample clause(stored in the TableSample class).
 *
 */
public class SamplePruner {

  /**
   * Table alias for the table e.g. in case of FROM t TABLESAMPLE(1 OUT OF 2 ON rand()) a
   * "a" is the table alias
   */
  private String tabAlias;
  
  /**
   * The parse tree corresponding to the TABLESAMPLE clause. e.g. in case of 
   * FROM t TABLESAMPLE(1 OUT OF 2 ON rand()) a the parse tree of 
   * "TABLESAMPLE(1 OUT OF 2 ON rand())" is parsed out and stored in tableSample
   */  
  private TableSample tableSample;
 
  /**
   * The log handle for this class
   */
  @SuppressWarnings("nls")
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.SamplePruner");

  /**
   * Constructs the SamplePruner given the table alias and the table sample
   * 	
   * @param alias The alias of the table specified in the query
   * @param tableSample The parse infromation of the TABLESAMPLE clause
   */
  public SamplePruner(String alias, TableSample tableSample) {
    this.tabAlias = alias;
    this.tableSample = tableSample;
  }
  
  /**
   * Gets the table alias
   * 
   * @return String
   */
  public String getTabAlias() {
    return this.tabAlias;
  }
  
  /**
   * Sets the table alias
   * 
   * @param tabAlias The table alias as specified in the query
   */
  public void setTabAlias(String tabAlias) {
    this.tabAlias = tabAlias;
  }
  
  /**
   * Gets the parse information of the associated table sample clause
   * 
   * @return TableSample
   */
  public TableSample getTableSample() {
    return this.tableSample;
  }
  
  /**
   * Sets the parse information of the associated table sample clause
   * 
   * @param tableSample Information related to the table sample clause
   */
  public void setTableSample(TableSample tableSample) {
    this.tableSample = tableSample;
  }

  /**
   * Prunes to get all the files in the partition that satisfy the TABLESAMPLE clause
   * 
   * @param part The partition to prune
   * @return Path[]
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  public Path[] prune(Partition part) throws SemanticException {
    int num = this.tableSample.getNumerator();
    int den = this.tableSample.getDenominator();
    int bucketCount = part.getBucketCount();
    String fullScanMsg = "";
    // check if input pruning is possible
    if (this.tableSample.getInputPruning()) {
      LOG.trace("numerator = " + num);
      LOG.trace("denominator = " + den);
      LOG.trace("bucket count = " + bucketCount);
      if (bucketCount == den) {
            Path [] ret = new Path [1];
            ret[0] = part.getBucketPath(num-1);
            return(ret);
      }
      else if (bucketCount > den && bucketCount % den == 0) {
        int numPathsInSample = bucketCount / den;
        Path [] ret = new Path[numPathsInSample];
        for (int i = 0; i < numPathsInSample; i++) {
          ret[i] = part.getBucketPath(i*den+num-1);
        }
        return ret;
      }
      else if (bucketCount < den && den % bucketCount == 0) {
        Path [] ret = new Path[1];
        ret[0] = part.getBucketPath((num-1)%bucketCount);
        return ret;
      }
      else {
        // need to do full scan
        fullScanMsg = "Tablesample denominator " 
            + den + " is not multiple/divisor of bucket count " 
            + bucketCount + " of table " + this.tabAlias;
      }
    }
    else {
      // need to do full scan
      fullScanMsg = "Tablesample not on clustered columns";
    }
    LOG.warn(fullScanMsg + ", using full table scan");
    Path [] ret = part.getPath();
    return ret;
  }
}
