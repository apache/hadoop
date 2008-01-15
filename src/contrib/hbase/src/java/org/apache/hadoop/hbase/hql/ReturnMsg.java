/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.hql;

import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Message returned when a {@link Command} is
 * {@link Command#execute(HBaseConfiguration)}'ed.
 */
public class ReturnMsg {
  private final String msg;
  private final int type;

  public ReturnMsg(int i, String string) {
    this.type = i;
    this.msg = string;
  }

  public ReturnMsg(int i) {
    this.type = i;
    this.msg = "";
  }

  public String getMsg() {
    return this.msg;
  }

  public int getType() {
    return this.type;
  }

  @Override
  public String toString() {
    return this.msg;
  }
}
