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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class TestOperators extends TestCase {

  // this is our row to test expressions on
  protected InspectableObject [] r;

  protected void setUp() {
    r = new InspectableObject [5];
    ArrayList<String> names = new ArrayList<String>(3);
    names.add("col0");
    names.add("col1");
    names.add("col2");
    ArrayList<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(3);
    objectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
    objectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
    objectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
    for(int i=0; i<5; i++) {
      ArrayList<String> data = new ArrayList<String> ();
      data.add(""+i);
      data.add(""+(i+1));
      data.add(""+(i+2));
      try {
        r[i] = new InspectableObject();
        r[i].o = data;
        r[i].oi = ObjectInspectorFactory.getStandardStructObjectInspector(names, objectInspectors);
      } catch (Throwable e) {
        throw new RuntimeException (e);
      }
    }
  }

  public void testBaseFilterOperator() throws Throwable {
    try {
      System.out.println("Testing Filter Operator");
      exprNodeDesc col0 = new exprNodeColumnDesc(String.class, "col0");
      exprNodeDesc col1 = new exprNodeColumnDesc(String.class, "col1");
      exprNodeDesc col2 = new exprNodeColumnDesc(String.class, "col2");
      exprNodeDesc zero = new exprNodeConstantDesc(Number.class, Long.valueOf(0));
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc(">", col2, col1);
      exprNodeDesc func2 = SemanticAnalyzer.getFuncExprNodeDesc("==", col0, zero);
      exprNodeDesc func3 = SemanticAnalyzer.getFuncExprNodeDesc("&&", func1, func2); 
      assert(func3 != null);
      filterDesc filterCtx = new filterDesc(func3);

      // Configuration
      Operator<filterDesc> op = OperatorFactory.get(filterDesc.class);
      op.setConf(filterCtx);

      // runtime initialization
      op.initialize(null);

      for(InspectableObject oner: r) {
        op.process(oner.o, oner.oi);
      }

      Map<Enum<?>, Long> results = op.getStats();
      System.out.println("filtered = " + results.get(FilterOperator.Counter.FILTERED));
      assertEquals(results.get(FilterOperator.Counter.FILTERED), Long.valueOf(4));
      System.out.println("passed = " + results.get(FilterOperator.Counter.PASSED));
      assertEquals(results.get(FilterOperator.Counter.PASSED), Long.valueOf(1));

      /*
      for(Enum e: results.keySet()) {
        System.out.println(e.toString() + ":" + results.get(e));
      }
      */
      System.out.println("Filter Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testFileSinkOperator() throws Throwable {
    try {
      System.out.println("Testing FileSink Operator");
      // col1
      exprNodeDesc exprDesc1 = new exprNodeColumnDesc(TypeInfoFactory.getPrimitiveTypeInfo(String.class),
          "col1");

      // col2
      ArrayList<exprNodeDesc> exprDesc2children = new ArrayList<exprNodeDesc>();
      exprNodeDesc expr1 = new exprNodeColumnDesc(String.class, "col0");
      exprNodeDesc expr2 = new exprNodeConstantDesc("1");
      exprNodeDesc exprDesc2 = SemanticAnalyzer.getFuncExprNodeDesc("concat", expr1, expr2);

      // select operator to project these two columns
      ArrayList<exprNodeDesc> earr = new ArrayList<exprNodeDesc> ();
      earr.add(exprDesc1);
      earr.add(exprDesc2);
      selectDesc selectCtx = new selectDesc(earr);
      Operator<selectDesc> op = OperatorFactory.get(selectDesc.class);
      op.setConf(selectCtx);

      // fileSinkOperator to dump the output of the select
      fileSinkDesc fsd = new fileSinkDesc ("file:///tmp" + File.separator + System.getProperty("user.name") + File.separator + "TestFileSinkOperator",
                                           Utilities.defaultTd);
      Operator<fileSinkDesc> flop = OperatorFactory.get(fileSinkDesc.class);
      flop.setConf(fsd);
      ArrayList<Operator<? extends Serializable>> nextOp = new ArrayList<Operator<? extends Serializable>> ();
      nextOp.add(flop);

      op.setChildOperators(nextOp);
      op.initialize(new JobConf(TestOperators.class));

      // evaluate on row
      for(int i=0; i<5; i++) {
        op.process(r[i].o, r[i].oi);
      }
      op.close(false);

      System.out.println("FileSink Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }


  public void testScriptOperator() throws Throwable {
    try {
      System.out.println("Testing Script Operator");
      // col1
      exprNodeDesc exprDesc1 = new exprNodeColumnDesc(String.class, "col1");

      // col2
      exprNodeDesc expr1 = new exprNodeColumnDesc(String.class, "col0");
      exprNodeDesc expr2 = new exprNodeConstantDesc("1");
      exprNodeDesc exprDesc2 = SemanticAnalyzer.getFuncExprNodeDesc("concat", expr1, expr2);

      // select operator to project these two columns
      ArrayList<exprNodeDesc> earr = new ArrayList<exprNodeDesc> ();
      earr.add(exprDesc1);
      earr.add(exprDesc2);
      selectDesc selectCtx = new selectDesc(earr);
      Operator<selectDesc> op = OperatorFactory.get(selectDesc.class);
      op.setConf(selectCtx);

      // scriptOperator to echo the output of the select
      tableDesc scriptOutput = PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "a,b");
      tableDesc scriptInput  = PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "a,b");
      scriptDesc sd = new scriptDesc("cat", scriptOutput, scriptInput);
      Operator<scriptDesc> sop = OperatorFactory.get(scriptDesc.class);
      sop.setConf(sd);
      ArrayList<Operator<? extends Serializable>> nextScriptOp = new ArrayList<Operator<? extends Serializable>> ();
      nextScriptOp.add(sop);


      // Collect operator to observe the output of the script
      collectDesc cd = new collectDesc (Integer.valueOf(10));
      CollectOperator cdop = (CollectOperator) OperatorFactory.get(collectDesc.class);
      cdop.setConf(cd);
      ArrayList<Operator<? extends Serializable>> nextCollectOp = new ArrayList<Operator<? extends Serializable>> ();
      nextCollectOp.add(cdop);


      // chain the scriptOperator to the select operator
      op.setChildOperators(nextScriptOp);
      // chain the collect operator to the script operator
      sop.setChildOperators(nextCollectOp);


      op.initialize(new JobConf(TestOperators.class));

      // evaluate on row
      for(int i=0; i<5; i++) {
        op.process(r[i].o, r[i].oi);
      }
      op.close(false);

      InspectableObject io = new InspectableObject();
      for(int i=0; i<5; i++) {
        cdop.retrieve(io);
        System.out.println("[" + i + "] io.o=" + io.o);
        System.out.println("[" + i + "] io.oi=" + io.oi);
        StructObjectInspector soi = (StructObjectInspector)io.oi;
        assert(soi != null);
        StructField a = soi.getStructFieldRef("a");
        StructField b = soi.getStructFieldRef("b");
        assertEquals(""+(i+1), soi.getStructFieldData(io.o, a));
        assertEquals((i) + "1", soi.getStructFieldData(io.o, b));
      }

      System.out.println("Script Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testMapOperator() throws Throwable {
    try {
      System.out.println("Testing Map Operator");
      // initialize configuration
      Configuration hconf = new JobConf(TestOperators.class);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HADOOPMAPFILENAME, "hdfs:///testDir/testFile");

      // initialize pathToAliases
      ArrayList<String> aliases = new ArrayList<String> ();
      aliases.add("a");
      aliases.add("b");
      LinkedHashMap<String, ArrayList<String>> pathToAliases = new LinkedHashMap<String, ArrayList<String>> ();
      pathToAliases.put("/testDir", aliases);

      // initialize pathToTableInfo
      // Default: treat the table as a single column "col" 
      tableDesc td = Utilities.defaultTd;
      partitionDesc pd = new partitionDesc(td, null);
      LinkedHashMap<String,org.apache.hadoop.hive.ql.plan.partitionDesc> pathToPartitionInfo = new
        LinkedHashMap<String,org.apache.hadoop.hive.ql.plan.partitionDesc> ();
      pathToPartitionInfo.put("/testDir", pd);

      // initialize aliasToWork
      collectDesc cd = new collectDesc (Integer.valueOf(1));
      CollectOperator cdop1 = (CollectOperator) OperatorFactory.get(collectDesc.class);
      cdop1.setConf(cd);
      CollectOperator cdop2 = (CollectOperator) OperatorFactory.get(collectDesc.class);
      cdop2.setConf(cd);
      HashMap<String,Operator<? extends Serializable>> aliasToWork = new HashMap<String,Operator<? extends Serializable>> ();
      aliasToWork.put("a", cdop1);
      aliasToWork.put("b", cdop2);

      // initialize mapredWork
      mapredWork mrwork = new mapredWork ();
      mrwork.setPathToAliases(pathToAliases);
      mrwork.setPathToPartitionInfo(pathToPartitionInfo);
      mrwork.setAliasToWork(aliasToWork);

      // get map operator and initialize it
      MapOperator mo = new MapOperator();
      mo.setConf(mrwork);
      mo.initialize(hconf);

      Text tw = new Text();
      InspectableObject io1 = new InspectableObject();
      InspectableObject io2 = new InspectableObject();
      for(int i=0; i<5; i++) {
        String answer = "[[" + i + ", " + (i+1) + ", " + (i+2) + "]]";
        
        tw.set("" + i + "\u0001" + (i+1) + "\u0001"+ (i+2));
        mo.process((Writable)tw);
        cdop1.retrieve(io1);
        cdop2.retrieve(io2);
        System.out.println("io1.o.toString() = " + io1.o.toString());
        System.out.println("io2.o.toString() = " + io2.o.toString());
        System.out.println("answer.toString() = " + answer.toString());
        assertEquals(answer.toString(), io1.o.toString());
        assertEquals(answer.toString(), io2.o.toString());
      }

      System.out.println("Map Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw (e);
    }
  }
}
