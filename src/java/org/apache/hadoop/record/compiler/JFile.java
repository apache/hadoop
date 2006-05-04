/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author milindb@yahoo-inc.com
 */
public class JFile {
    
    private String mName;
    private ArrayList mInclFiles;
    private ArrayList mRecords;
    
    /** Creates a new instance of JFile */
    public JFile(String name, ArrayList inclFiles, ArrayList recList) {
        mName = name;
        mInclFiles = inclFiles;
        mRecords = recList;
    }
        
    String getName() {
        int idx = mName.lastIndexOf('/');
        return (idx > 0) ? mName.substring(idx) : mName; 
    }
    
    public void genCode(String language) throws IOException {
        if ("c++".equals(language)) {
            CppGenerator gen = new CppGenerator(mName, mInclFiles, mRecords);
            gen.genCode();
        } else if ("java".equals(language)) {
            JavaGenerator gen = new JavaGenerator(mName, mInclFiles, mRecords);
            gen.genCode();
        } else {
            System.out.println("Cannnot recognize language:"+language);
            System.exit(1);
        }
    }
}
