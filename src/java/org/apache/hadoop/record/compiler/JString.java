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

/**
 *
 * @author Milind Bhandarkar
 */
public class JString extends JCompType {
    
    /** Creates a new instance of JString */
    public JString() {
        super(" ::std::string", "String", "String", "String");
    }
    
    public String getSignature() {
        return "s";
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    String "+fname+";\n";
        }
        return ret + "        "+fname+"=a_.readString(\""+tag+"\");\n";
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.writeString("+fname+",\""+tag+"\");\n";
    }
    
    
}
