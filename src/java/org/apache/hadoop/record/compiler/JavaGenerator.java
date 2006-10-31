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

package org.apache.hadoop.record.compiler;

import java.util.ArrayList;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 * Java Code generator front-end for Hadoop record I/O.
 *
 * @author Milind Bhandarkar
 */
class JavaGenerator {
    private String mName;
    private ArrayList mInclFiles;
    private ArrayList mRecList;
    
    /** Creates a new instance of JavaGenerator
     *
     * @param name possibly full pathname to the file
     * @param incl included files (as JFile)
     * @param records List of records defined within this file
     */
    JavaGenerator(String name, ArrayList incl, ArrayList records) {
        mName = name;
        mInclFiles = incl;
        mRecList = records;
    }
    
    /**
     * Generate Java code for records. This method is only a front-end to
     * JRecord, since one file is generated for each record.
     */
    void genCode() throws IOException {
        for (Iterator i = mRecList.iterator(); i.hasNext(); ) {
            JRecord rec = (JRecord) i.next();
            rec.genJavaCode();
        }
    }
}
