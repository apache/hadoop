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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;


public class UDFOPBitXor implements UDF {

  private static Log LOG = LogFactory.getLog(UDFOPBitXor.class.getName());

  public UDFOPBitXor() {
  }

  public Byte evaluate(Byte a, Byte b)  {
    if ((a == null) || (b == null))
      return null;

    return Byte.valueOf((byte)(a.byteValue() ^ b.byteValue()));
  }

  public Integer evaluate(Integer a, Integer b)  {
    if ((a == null) || (b == null))
      return null;

    return Integer.valueOf(a.intValue() ^ b.intValue());
  }

  public Long evaluate(Long a, Long b)  {
    if ((a == null) || (b == null))
      return null;

    return Long.valueOf(a.longValue() ^ b.longValue());
  }
  
  public Long evaluate(String a, String b)  {
    if ((a == null) || (b == null))
      return null;

    return evaluate(Long.valueOf(a), Long.valueOf(b));
  }
  
  public Long evaluate(Long a, String b)  {
    if ((a == null) || (b == null))
      return null;

    return evaluate(a, Long.valueOf(b));
  }
  
  public Long evaluate(String a, Long b)  {
    if ((a == null) || (b == null))
      return null;

    return evaluate(Long.valueOf(a), b);
  }

}
