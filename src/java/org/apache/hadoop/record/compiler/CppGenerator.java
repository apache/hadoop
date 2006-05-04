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

import java.util.ArrayList;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author milindb
 */
class CppGenerator {
    private String mName;
    private ArrayList mInclFiles;
    private ArrayList mRecList;
    
    /** Creates a new instance of CppGenerator */
    public CppGenerator(String name, ArrayList ilist, ArrayList rlist) {
        mName = name;
        mInclFiles = ilist;
        mRecList = rlist;
    }
    
    public void genCode() throws IOException {
        FileWriter cc = new FileWriter(mName+".cc");
        FileWriter hh = new FileWriter(mName+".hh");
        hh.write("#ifndef __"+mName.toUpperCase().replace('.','_')+"__\n");
        hh.write("#define __"+mName.toUpperCase().replace('.','_')+"__\n");
        
        hh.write("#include \"recordio.hh\"\n");
        for (Iterator i = mInclFiles.iterator(); i.hasNext();) {
            JFile f = (JFile) i.next();
            hh.write("#include \""+f.getName()+".hh\"\n");
        }
        cc.write("#include \""+mName+".hh\"\n");
        
        for (Iterator i = mRecList.iterator(); i.hasNext();) {
            JRecord jr = (JRecord) i.next();
            jr.genCppCode(hh, cc);
        }
        
        hh.write("#endif //"+mName.toUpperCase().replace('.','_')+"__\n");
        
        hh.close();
        cc.close();
    }
}
