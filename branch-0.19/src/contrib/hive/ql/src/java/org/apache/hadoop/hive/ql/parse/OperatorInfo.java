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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.RowResolver;

/**
 * Implementation of OperatorInfo which bundles the operator and its output row resolver
 *
 **/

public class OperatorInfo implements Cloneable {
    private Operator<?> op;
    private RowResolver rr;
    
    public OperatorInfo(Operator<?> op, RowResolver rr) {
      this.op = op;
      this.rr = rr;
    }
    
    public Object clone() {
      return new OperatorInfo(op, rr);
    }
    
    public Operator<?> getOp() {
      return op;
    }

    public void setOp(Operator<?> op) {
      this.op = op;
    }

    public RowResolver getRowResolver() {
      return rr;
    }

    public void setRowResolver(RowResolver rr) {
      this.rr = rr;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      String terminal_str = op.toString();
      sb.append(terminal_str.substring(terminal_str.lastIndexOf('.')+1));
      sb.append("[");
      sb.append(rr.toString());
      sb.append("]");
      return sb.toString();
    }
}

