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

package org.apache.hadoop.hive.serde.dynamic_type;

import java.util.*;

public class DynamicSerDeTypeContainer {

    public Map<String, Object> fields;

    public DynamicSerDeTypeContainer() {
        fields = new HashMap<String, Object>();
    }

    public Map getFields() {
        return fields;
    }

    public Iterator keySet() {
        return fields.keySet().iterator();
    }

    public Iterator entrySet() {
        return fields.entrySet().iterator();
    }

    public String toString() {
        StringBuffer ret = new StringBuffer();
        String comma = "";
        for(Iterator it = fields.keySet().iterator(); it.hasNext(); ) {
            ret.append(comma + fields.get(it.next()));
            comma = ",";
        }
        return ret.toString();
    }
}
