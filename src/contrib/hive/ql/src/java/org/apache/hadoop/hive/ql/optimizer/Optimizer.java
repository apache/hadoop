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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of the optimizer
 */
public class Optimizer {
	private ParseContext pctx;
	private List<Transform> transformations;
	
	/**
	 * empty constructor
	 */
	public Optimizer() {
	}

	/**
	 * create the list of transformations
	 */
	public void initialize() {
		transformations = new ArrayList<Transform>();
		transformations.add(new ColumnPruner());
	}
	
	/**
	 * invoke all the transformations one-by-one, and alter the query plan
	 * @return ParseContext
	 * @throws SemanticException
	 */
	public ParseContext optimize() throws SemanticException {
		for (Transform t : transformations)
			pctx = t.transform(pctx);
    return pctx;
	}
	
	/**
	 * @return the pctx
	 */
	public ParseContext getPctx() {
		return pctx;
	}

	/**
	 * @param pctx the pctx to set
	 */
	public void setPctx(ParseContext pctx) {
		this.pctx = pctx;
	}
	
	
}
