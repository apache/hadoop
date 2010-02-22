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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Extends the basic <code>Reducer</code> class to add the required key and
 * value input/output classes. While the input key and value as well as the 
 * output key can be anything handed in from the previous map phase the output 
 * value <u>must</u> be either a {@link org.apache.hadoop.hbase.client.Put Put} 
 * or a {@link org.apache.hadoop.hbase.client.Delete Delete} instance when
 * using the {@link TableOutputFormat} class.
 * <p>
 * This class is extended by {@link IdentityTableReducer} but can also be 
 * subclassed to implement similar features or any custom code needed. It has
 * the advantage to enforce the output value to a specific basic type. 
 * 
 * @param <KEYIN>  The type of the input key.
 * @param <VALUEIN>  The type of the input value.
 * @param <KEYOUT>  The type of the output key.
 * @see org.apache.hadoop.mapreduce.Reducer
 */
public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT>
extends Reducer<KEYIN, VALUEIN, KEYOUT, Writable> {
}