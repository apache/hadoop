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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.createFunctionDesc;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.OperatorType;

public class FunctionTask extends Task<FunctionWork> {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog("hive.ql.exec.FunctionTask");
  
  transient HiveConf conf;
  
  public void initialize(HiveConf conf) {
    super.initialize(conf);
    this.conf = conf;
  }
  
  @Override
  public int execute() {
    createFunctionDesc createFunctionDesc = work.getCreateFunctionDesc();
    if (createFunctionDesc != null) {
      try {
        Class<? extends UDF> udfClass = getUdfClass(createFunctionDesc);
        FunctionRegistry.registerUDF(createFunctionDesc.getFunctionName(), udfClass,
                                     OperatorType.PREFIX, false);
        return 0;
      } catch (ClassNotFoundException e) {
        LOG.info("create function: " + StringUtils.stringifyException(e));
        return 1;
      }
    }
    return 0;
  }

  @SuppressWarnings("unchecked")
  private Class<? extends UDF> getUdfClass(createFunctionDesc desc)
      throws ClassNotFoundException {
    return (Class<? extends UDF>) conf.getClassByName(desc.getClassName());
  }

}
