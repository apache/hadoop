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
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;


public class TestPlan extends TestCase {

  public void testPlan() throws Exception {

    final String F1 = "#affiliations";
    final String F2 = "friends[0].friendid";

    try {
      // initialize a complete map reduce configuration
      exprNodeDesc expr1 = new exprNodeColumnDesc(TypeInfoFactory.getPrimitiveTypeInfo(String.class), F1);
      exprNodeDesc expr2 = new exprNodeColumnDesc(TypeInfoFactory.getPrimitiveTypeInfo(String.class), F2);
      exprNodeDesc filterExpr = SemanticAnalyzer.getFuncExprNodeDesc("==", expr1, expr2);

      filterDesc filterCtx = new filterDesc(filterExpr);
      Operator<filterDesc> op = OperatorFactory.get(filterDesc.class);
      op.setConf(filterCtx);

      ArrayList<String> aliasList = new ArrayList<String> ();
      aliasList.add("a");
      LinkedHashMap<String, ArrayList<String>> pa = new LinkedHashMap<String, ArrayList<String>> ();
      pa.put("/tmp/testfolder", aliasList);

      tableDesc tblDesc = Utilities.defaultTd;
      partitionDesc partDesc = new partitionDesc(tblDesc, null);
      LinkedHashMap<String, partitionDesc> pt = new LinkedHashMap<String, partitionDesc> ();
      pt.put("/tmp/testfolder", partDesc);

      HashMap<String, Operator<? extends Serializable>> ao = new HashMap<String, Operator<? extends Serializable>> ();
      ao.put("a", op);

      mapredWork mrwork = new mapredWork();
      mrwork.setPathToAliases(pa);
      mrwork.setPathToPartitionInfo(pt);
      mrwork.setAliasToWork(ao);

      // serialize the configuration once ..
      ByteArrayOutputStream baos = new ByteArrayOutputStream ();
      Utilities.serializeMapRedWork(mrwork, baos);
      baos.close();
      String v1 = baos.toString();

      // store into configuration
      JobConf job = new JobConf(TestPlan.class);
      job.set("fs.default.name", "file:///");
      Utilities.setMapRedWork(job, mrwork);
      mapredWork mrwork2 = Utilities.getMapRedWork(job);
      Utilities.clearMapRedWork(job);

      // over here we should have some checks of the deserialized object against the orginal object
      //System.out.println(v1);

      // serialize again
      baos.reset();
      Utilities.serializeMapRedWork(mrwork2, baos);
      baos.close();

      // verify that the two are equal
      assertEquals(v1, baos.toString());

    } catch (Exception excp) {
      excp.printStackTrace();
      throw excp;
    }
    System.out.println("Serialization/Deserialization of plan successful");
  }
}
