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

package org.apache.hadoop.hive.serde2.thrift;

import com.facebook.thrift.TException;

/**
 * An interface for TProtocols that actually write out nulls - 
 * This should be for all those that don't actually use
 * fieldids in the written data like TCTLSeparatedProtocol.
 * 
 */
public interface WriteNullsProtocol {
  /**
   * Was the last primitive read really a NULL. Need
   * only be called when the value of the primitive
   * was 0. ie the protocol should return 0 on nulls
   * and the caller will then check if it was actually null
   * For boolean this is false.
   */
  public boolean lastPrimitiveWasNull() throws TException;

  /**
   * Write a null 
   */
  public void writeNull() throws TException;

}
