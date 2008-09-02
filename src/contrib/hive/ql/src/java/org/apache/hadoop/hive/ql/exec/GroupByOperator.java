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

package org.apache.hadoop.hive.ql.exec;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.serde.SerDeField;
import org.apache.hadoop.conf.Configuration;

/**
 * GroupBy operator implementation.
 */
public class GroupByOperator extends Operator <groupByDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] keyFields;
  transient protected ExprNodeEvaluator[][] aggregationParameterFields;
  // In the future, we may allow both count(DISTINCT a) and sum(DISTINCT a) in the same SQL clause,
  // so aggregationIsDistinct is a boolean array instead of a single number. 
  transient protected boolean[] aggregationIsDistinct;

  transient Class<? extends UDAF>[] aggregationClasses; 
  transient protected Method[] aggregationsAggregateMethods;
  transient protected Method[] aggregationsEvaluateMethods;

  transient protected List<SerDeField> choKeyFields;

  // Used by sort-based GroupBy: Mode = COMPLETE, PARTIAL1, PARTIAL2
  transient protected CompositeHiveObject currentKeys;
  transient protected UDAF[] aggregations;
  transient protected Object[][] aggregationsParametersLastInvoke;

  // Used by hash-based GroupBy: Mode = HASH
  transient protected HashMap<CompositeHiveObject, UDAF[]> hashAggregations;
  
  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      // init keyFields
      keyFields = new ExprNodeEvaluator[conf.getKeys().size()];
      for (int i = 0; i < keyFields.length; i++) {
        keyFields[i] = ExprNodeEvaluatorFactory.get(conf.getKeys().get(i));
      }
    
      // init aggregationParameterFields
      aggregationParameterFields = new ExprNodeEvaluator[conf.getAggregators().size()][];
      for (int i = 0; i < aggregationParameterFields.length; i++) {
        ArrayList<exprNodeDesc> parameters = conf.getAggregators().get(i).getParameters();
        aggregationParameterFields[i] = new ExprNodeEvaluator[parameters.size()];
        for (int j = 0; j < parameters.size(); j++) {
          aggregationParameterFields[i][j] = ExprNodeEvaluatorFactory.get(parameters.get(j));
        }
      }
      // init aggregationIsDistinct
      aggregationIsDistinct = new boolean[conf.getAggregators().size()];
      for(int i=0; i<aggregationIsDistinct.length; i++) {
        aggregationIsDistinct[i] = conf.getAggregators().get(i).getDistinct();
      }

      // init aggregationClasses  
      aggregationClasses = (Class<? extends UDAF>[]) new Class[conf.getAggregators().size()];
      for (int i = 0; i < conf.getAggregators().size(); i++) {
        aggregationDesc agg = conf.getAggregators().get(i);
        aggregationClasses[i] = agg.getAggregationClass();
      }

      // init aggregations, aggregationsAggregateMethods,
      // aggregationsEvaluateMethods
      aggregationsAggregateMethods = new Method[aggregationClasses.length];
      aggregationsEvaluateMethods = new Method[aggregationClasses.length];
      String aggregateMethodName = (conf.getMode() == groupByDesc.Mode.PARTIAL2 
         ? "aggregatePartial" : "aggregate");
      String evaluateMethodName = ((conf.getMode() == groupByDesc.Mode.PARTIAL1 || conf.getMode() == groupByDesc.Mode.HASH)
         ? "evaluatePartial" : "evaluate");
      for(int i=0; i<aggregationClasses.length; i++) {
        // aggregationsAggregateMethods
        for( Method m : aggregationClasses[i].getMethods() ){
          if( m.getName().equals( aggregateMethodName ) 
              && m.getParameterTypes().length == aggregationParameterFields[i].length) {              
            aggregationsAggregateMethods[i] = m;
            break;
          }
        }
        if (null == aggregationsAggregateMethods[i]) {
          throw new RuntimeException("Cannot find " + aggregateMethodName + " method of UDAF class "
                                   + aggregationClasses[i].getName() + " that accepts "
                                   + aggregationParameterFields[i].length + " parameters!");
        }
        // aggregationsEvaluateMethods
        aggregationsEvaluateMethods[i] = aggregationClasses[i].getMethod(evaluateMethodName);

        if (null == aggregationsEvaluateMethods[i]) {
          throw new RuntimeException("Cannot find " + evaluateMethodName + " method of UDAF class "
                                   + aggregationClasses[i].getName() + "!");
        }
        assert(aggregationsEvaluateMethods[i] != null);
      }

      if (conf.getMode() != groupByDesc.Mode.HASH) {
        aggregationsParametersLastInvoke = new Object[conf.getAggregators().size()][];
        aggregations = newAggregations();
      } else {
        hashAggregations = new HashMap<CompositeHiveObject, UDAF[]>();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  protected UDAF[] newAggregations() throws Exception {      
    UDAF[] aggs = new UDAF[aggregationClasses.length];
    for(int i=0; i<aggregationClasses.length; i++) {
      aggs[i] = aggregationClasses[i].newInstance();
      aggs[i].init();
    }
    return aggs;
  }

  protected void updateAggregations(UDAF[] aggs, HiveObject row, Object[][] lastInvoke) throws Exception {
    for(int ai=0; ai<aggs.length; ai++) {
      // Calculate the parameters 
      Object[] o = new Object[aggregationParameterFields[ai].length];
      for(int pi=0; pi<aggregationParameterFields[ai].length; pi++) {
        o[pi] = aggregationParameterFields[ai][pi].evaluateToObject(row);
      }
      // Update the aggregations.
      if (aggregationIsDistinct[ai] && lastInvoke != null) {
        // different differentParameters?
        boolean differentParameters = (lastInvoke[ai] == null);
        if (!differentParameters) {
          for(int pi=0; pi<o.length; pi++) {
            if (!o[pi].equals(lastInvoke[ai][pi])) {
              differentParameters = true;
              break;
            }
          }
        }  
        if (differentParameters) {
          aggregationsAggregateMethods[ai].invoke(aggs[ai], o);
          lastInvoke[ai] = o;
        }
      } else {
        aggregationsAggregateMethods[ai].invoke(aggs[ai], o);
      }
    }
  }
  
  public void process(HiveObject row) throws HiveException {
    
    try {
      // Compute the keys
      ArrayList<HiveObject> keys = new ArrayList<HiveObject>(keyFields.length);
      for (int i = 0; i < keyFields.length; i++) {
        keys.add(keyFields[i].evaluate(row));
      }
      CompositeHiveObject newKeys = new CompositeHiveObject(keys); 
      
      // Prepare aggs for updating
      UDAF[] aggs = null;
      Object[][] lastInvoke = null;
      if (aggregations != null) {
        // sort-based aggregation
        // Need to forward?
        if (currentKeys != null && !newKeys.equals(currentKeys)) {
            forward(currentKeys, aggregations);
        }
        // Need to update the keys?
        if (currentKeys == null || !newKeys.equals(currentKeys)) {
            currentKeys = newKeys;
            // init aggregations
            for(UDAF aggregation: aggregations) {
                aggregation.init();
            }
            // clear parameters in last-invoke
            for(int i=0; i<aggregationsParametersLastInvoke.length; i++) {
              aggregationsParametersLastInvoke[i] = null;
            }
        }
        aggs = aggregations;
        lastInvoke = aggregationsParametersLastInvoke;
      } else {
        // hash-based aggregations
        aggs = hashAggregations.get(newKeys);
        if (aggs == null) {
          aggs = newAggregations();
          hashAggregations.put(newKeys, aggs);
          // TODO: Hash aggregation does not support DISTINCT now
          lastInvoke = null;
        }
      }

      // Update the aggs
      updateAggregations(aggs, row, lastInvoke);

    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }
  
  /**
   * Forward a record of keys and aggregation results.
   * 
   * @param keys
   *          The keys in the record
   * @throws HiveException
   */
  protected void forward(CompositeHiveObject keys, UDAF[] aggs) throws Exception {
    if (choKeyFields == null) {
      // init choKeyFields
      choKeyFields = new ArrayList<SerDeField>();
      for (int i = 0; i < keyFields.length; i++) {
        choKeyFields.add(keys.getFieldFromExpression(Integer.valueOf(i).toString()));
      }
    }
    int totalFields = keys.width + aggs.length;
    CompositeHiveObject cho = new CompositeHiveObject(totalFields);
    for (int i = 0; i < keys.width; i++) {
      cho.addHiveObject(keys.get(choKeyFields.get(i)));
    }
    for (int i = 0; i < aggs.length; i++) {
      cho.addHiveObject(new PrimitiveHiveObject(aggregationsEvaluateMethods[i].invoke(aggs[i])));
    }
    forward(cho);
  }
  
  /**
   * We need to forward all the aggregations to children.
   * 
   */
  public void close(boolean abort) throws HiveException {
    if (!abort) {
      try {
        if (aggregations != null) {
          // sort-based aggregations
          if (currentKeys != null) {
            forward(currentKeys, aggregations);
          }
        } else if (hashAggregations != null) {
          // hash-based aggregations
          for (CompositeHiveObject key: hashAggregations.keySet()) {
            forward(key, hashAggregations.get(key));
          }
        } else {
          // The GroupByOperator is not initialized, which means there is no data
          // (since we initialize the operators when we see the first record).
          // Just do nothing here.
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
    super.close(abort);
  }
}
