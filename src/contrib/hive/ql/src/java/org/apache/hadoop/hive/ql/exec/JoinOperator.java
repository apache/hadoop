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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.joinCond;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.serde.SerDeField;
import org.apache.hadoop.conf.Configuration;

/**
 * Join operator implementation.
 */
public class JoinOperator extends Operator<joinDesc> implements Serializable {

  // a list of value expressions for each alias are maintained 
  public static class JoinExprMap {
    ExprNodeEvaluator[] valueFields;
    List<SerDeField> listFields;

    public JoinExprMap(ExprNodeEvaluator[] valueFields,
        List<SerDeField> listFields) {
      this.valueFields = valueFields;
      this.listFields = listFields;
    }

    public ExprNodeEvaluator[] getValueFields() {
      return valueFields;
    }

    public List<SerDeField> getListFields() {
      return listFields;
    }
  }

  public static class IntermediateObject{
    CompositeHiveObject[] objs;
    int curSize;

    public IntermediateObject(CompositeHiveObject[] objs, int curSize) {
      this.objs  = objs;
      this.curSize = curSize;
    }

    public CompositeHiveObject[] getObjs() { return objs; }
    public int getCurSize() { return curSize; }
    public void pushObj(CompositeHiveObject obj) { objs[curSize++] = obj; }
    public void popObj() { curSize--; }
  }

  transient protected int numValues; // number of aliases
  transient static protected ExprNodeEvaluator aliasField;
  transient protected HashMap<Byte, JoinExprMap> joinExprs;
  transient static protected Byte[] order; // order in which the results should be outputted
  transient protected joinCond[] condn;
  transient protected boolean noOuterJoin;
  transient private HiveObject[] dummyObj; // for outer joins, contains the potential nulls for the concerned aliases
  transient private Vector<CompositeHiveObject>[] dummyObjVectors;
  transient private Stack<Iterator<CompositeHiveObject>> iterators;
  transient private int totalSz; // total size of the composite object

  static
  {
    aliasField = ExprNodeEvaluatorFactory.get(new exprNodeColumnDesc(String.class, Utilities.ReduceField.ALIAS.toString()));
  }
  
  HashMap<Byte, Vector<CompositeHiveObject>> storage;

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    totalSz = 0;
    // Map that contains the rows for each alias
    storage = new HashMap<Byte, Vector<CompositeHiveObject>>();
    
    numValues = conf.getExprs().size();
    joinExprs = new HashMap<Byte, JoinExprMap>();
    if (order == null)
    {
      order = new Byte[numValues];
      for (int i = 0; i < numValues; i++)
        order[i] = (byte)i;
    }
    condn = conf.getConds();
    noOuterJoin = conf.getNoOuterJoin();
    Map<Byte, ArrayList<exprNodeDesc>> map = conf.getExprs();
    Iterator entryIter = map.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry e = (Map.Entry)entryIter.next();
      Byte key = (Byte)e.getKey();
      ArrayList<exprNodeDesc> expr = (ArrayList<exprNodeDesc>)e.getValue();
      int sz = expr.size();
      totalSz += sz;

      ExprNodeEvaluator[] valueFields = new ExprNodeEvaluator[sz];

      for (int j = 0; j < sz; j++)
        valueFields[j] = ExprNodeEvaluatorFactory.get(expr.get(j));

      joinExprs.put(key, new JoinExprMap(valueFields, CompositeHiveObject
          .getFields(sz)));
    }

    dummyObj = new HiveObject[numValues];
    dummyObjVectors = new Vector[numValues];

    int pos = 0;
    for (Byte alias : order) {
      int sz = map.get(alias).size();
      CompositeHiveObject nr = new CompositeHiveObject(sz);

      for (int j = 0; j < sz; j++)
        nr.addHiveObject(null);

      dummyObj[pos] = nr;
      Vector<CompositeHiveObject> values = new Vector<CompositeHiveObject>();
      values.add((CompositeHiveObject) dummyObj[pos]);
      dummyObjVectors[pos] = values;
      pos++;
    }

    iterators = new Stack<Iterator<CompositeHiveObject>>();
  }

  public void startGroup() throws HiveException {
    l4j.trace("Join: Starting new group");
    storage.clear();
    for (Byte alias : order)
      storage.put(alias, new Vector<CompositeHiveObject>());
  }

  public void process(HiveObject row) throws HiveException {
    try {
      // get alias
      Byte alias = (Byte) (aliasField.evaluate(row).getJavaObject());

      // get the expressions for that alias
      JoinExprMap exmap = joinExprs.get(alias);
      ExprNodeEvaluator[] valueFields = exmap.getValueFields();

      // Compute the values
      CompositeHiveObject nr = new CompositeHiveObject(valueFields.length);
      for (ExprNodeEvaluator vField : valueFields)
        nr.addHiveObject(vField.evaluate(row));

      // Add the value to the vector
      storage.get(alias).add(nr);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  private void createForwardJoinObject(IntermediateObject intObj, boolean[] nullsArr) throws HiveException {
    CompositeHiveObject nr = new CompositeHiveObject(totalSz);
    for (int i = 0; i < numValues; i++) {
      Byte alias = order[i];
      int sz = joinExprs.get(alias).getValueFields().length;
      if (nullsArr[i])
        for (int j = 0; j < sz; j++)
          nr.addHiveObject(null);
      else
      {
        List <SerDeField> fields = joinExprs.get(alias).getListFields();
        CompositeHiveObject obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++)
          nr.addHiveObject(obj.get(fields.get(j)));
      }
    }

    forward(nr);
  }

  private void copyOldArray(boolean[] src, boolean[] dest) {
    for (int i = 0; i < src.length; i++) dest[i] = src[i];
  }

  private Vector<boolean[]> joinObjectsInnerJoin(Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls, CompositeHiveObject newObj, IntermediateObject intObj, int left, boolean newObjNull)
  {
    if (newObjNull) return resNulls;
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }
  
  private Vector<boolean[]> joinObjectsLeftOuterJoin(Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls, CompositeHiveObject newObj, IntermediateObject intObj, int left, boolean newObjNull)
  {
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      copyOldArray(oldNulls, newNulls);
      if (oldObjNull)
        newNulls[oldNulls.length] = true;
      else
        newNulls[oldNulls.length] = newObjNull;
      resNulls.add(newNulls);
    }
    return resNulls;
  }

  private Vector<boolean[]> joinObjectsRightOuterJoin(Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls, CompositeHiveObject newObj, IntermediateObject intObj, int left, boolean newObjNull)
  {
    if (newObjNull) return resNulls;
    boolean allOldObjsNull = true;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left])
      {
        allOldObjsNull = false;
        break;
      }
    }

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      else if (allOldObjsNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        for (int i = 0; i < intObj.getCurSize() - 1; i++)
          newNulls[i] = true;
        newNulls[oldNulls.length] = newObjNull;
        return resNulls;
      }
    }
    return resNulls;
  }

  private Vector<boolean[]> joinObjectsFullOuterJoin(Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls, CompositeHiveObject newObj, IntermediateObject intObj, int left, boolean newObjNull)
  {
    if (newObjNull) {
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext())
      {
        boolean[] oldNulls = nullsIter.next();
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      return resNulls;
    }
    
    boolean allOldObjsNull = true;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left])
      {
        allOldObjsNull = false;
        break;
      }
    }
    boolean rhsPreserved = false;

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext())
    {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull)   
      {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      else if (oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = true;
        resNulls.add(newNulls);
         
        if (allOldObjsNull && !rhsPreserved) {
          newNulls = new boolean[intObj.getCurSize()];
          for (int i = 0; i < oldNulls.length; i++)
            newNulls[i] = true;
          newNulls[oldNulls.length] = false;
          resNulls.add(newNulls);
          rhsPreserved = true;
        }
      }
    }
    return resNulls;
  }

  /*
   * The new input is added to the list of existing inputs. Each entry in the 
   * array of inputNulls denotes the entries in the intermediate object to
   * be used. The intermediate object is augmented with the new object, and 
   * list of nulls is changed appropriately. The list will contain all non-nulls
   * for a inner join. The outer joins are processed appropriately.
   */
  private Vector<boolean[]> joinObjects(Vector<boolean[]> inputNulls, CompositeHiveObject newObj, IntermediateObject intObj, int joinPos)
  {
    Vector<boolean[]> resNulls = new Vector<boolean[]>();
    boolean newObjNull = newObj == dummyObj[joinPos] ? true : false;
    if (joinPos == 0)
    {
      if (newObjNull) return null;
      boolean[] nulls = new boolean[1];
      nulls[0] = newObjNull;
      resNulls.add(nulls);
      return resNulls;
    }
    
    int left = condn[joinPos - 1].getLeft();
    int type = condn[joinPos - 1].getType();
    
    // process all nulls for RIGHT and FULL OUTER JOINS
    if (((type == joinDesc.RIGHT_OUTER_JOIN) || (type == joinDesc.FULL_OUTER_JOIN)) 
        && !newObjNull && (inputNulls == null)) { 
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < newNulls.length - 1; i++)
        newNulls[i] = true;
      newNulls[newNulls.length-1] = false;
      resNulls.add(newNulls);
      return resNulls;
    }

    if (inputNulls == null)
      return null;

    if (type == joinDesc.INNER_JOIN) 
      return joinObjectsInnerJoin(resNulls, inputNulls, newObj, intObj, left, newObjNull);
    else if (type == joinDesc.LEFT_OUTER_JOIN) 
      return joinObjectsLeftOuterJoin(resNulls, inputNulls, newObj, intObj, left, newObjNull);
    else if (type == joinDesc.RIGHT_OUTER_JOIN) 
      return joinObjectsRightOuterJoin(resNulls, inputNulls, newObj, intObj, left, newObjNull);
    assert (type == joinDesc.FULL_OUTER_JOIN);
    return joinObjectsFullOuterJoin(resNulls, inputNulls, newObj, intObj, left, newObjNull);
  }
  
  /* 
   * genObject is a recursive function. For the inputs, a array of
   * bitvectors is maintained (inputNulls) where each entry denotes whether
   * the element is to be used or not (whether it is null or not). The size of
   * the bitvector is same as the number of inputs under consideration 
   * currently. When all inputs are accounted for, the output is forwared
   * appropriately.
   */
  private void genObject(Vector<boolean[]> inputNulls, int aliasNum, IntermediateObject intObj) 
    throws HiveException {
    if (aliasNum < numValues) {
      Iterator<CompositeHiveObject> aliasRes = storage.get(order[aliasNum])
        .iterator();
      iterators.push(aliasRes);
      while (aliasRes.hasNext()) {
        CompositeHiveObject newObj = aliasRes.next();
        intObj.pushObj(newObj);
        Vector<boolean[]> newNulls = joinObjects(inputNulls, newObj, intObj, aliasNum);
        genObject(newNulls, aliasNum + 1, intObj);
        intObj.popObj();
      }
      iterators.pop();
    }
    else {
      if (inputNulls == null) return;
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] nullsVec = nullsIter.next();
        createForwardJoinObject(intObj, nullsVec);
      }
    }
  }

  /**
   * Forward a record of join results.
   * 
   * @throws HiveException
   */
  public void endGroup() throws HiveException {
    try {
      l4j.trace("Join Op: endGroup called");

      // does any result need to be emitted
      for (int i = 0; i < numValues; i++) {
        Byte alias = order[i];
        if (storage.get(alias).iterator().hasNext() == false) {
          if (noOuterJoin)
            return;
          else
            storage.put(alias, dummyObjVectors[i]);
        }
      }

      genObject(null, 0, new IntermediateObject(new CompositeHiveObject[numValues], 0));
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  /**
   * All done
   * 
   */
  public void close(boolean abort) throws HiveException {
    l4j.trace("Join Op close");
    super.close(abort);
  }
}


