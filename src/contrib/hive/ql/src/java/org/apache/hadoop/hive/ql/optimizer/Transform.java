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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Optimizer interface. All the rule-based optimizations implement this interface. All the transformations are invoked sequentially. They take the current
 * parse context (which contains the operator tree among other things), perform all the optimizations, and then return the updated parse context.
 */
public interface Transform {
	/**
	 * All transformation steps implement this interface
	 * @param pctx input parse context
	 * @return ParseContext
	 * @throws SemanticException
	 */
	public ParseContext transform(ParseContext pctx) throws SemanticException;
}
