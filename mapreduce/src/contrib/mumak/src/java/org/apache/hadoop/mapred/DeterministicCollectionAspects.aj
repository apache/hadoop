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
package org.apache.hadoop.mapred;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;

// HashSet and HashMap do not guarantee that the oder of iteration is 
// determinstic. We need the latter for the deterministic replay of 
// simulations. These iterations are heavily used in the JobTracker, e.g. when 
// looking for non-local map tasks. Not all HashSet and HashMap instances
// are iterated over, but to be safe and simple we replace all with 
// LinkedHashSet and LinkedHashMap whose iteration order is deterministic.

public privileged aspect DeterministicCollectionAspects {

  // Fortunately the Java runtime type of generic classes do not contain
  // the generic parameter. We can catch all with a single base pattern using
  // the erasure of the generic type.

  HashSet around() : call(HashSet.new()) {
    return new LinkedHashSet();
  }

  HashMap around() : call(HashMap.new()) {
    return new LinkedHashMap();
  }
}
