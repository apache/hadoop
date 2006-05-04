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
 * @author milindb
 */
abstract class JCompType extends JType {
    
    /** Creates a new instance of JCompType */
    public JCompType(String cppType, String javaType, String suffix, String wrapper) {
        super(cppType, javaType, suffix, wrapper, null);
    }
    
    public String genCppGetSet(String fname, int fIdx) {
        String cgetFunc = "  virtual const "+getCppType()+"& get"+fname+"() const {\n";
        cgetFunc += "    return m"+fname+";\n";
        cgetFunc += "  }\n";
        String getFunc = "  virtual "+getCppType()+"& get"+fname+"() {\n";
        getFunc += "    bs_.set("+fIdx+");return m"+fname+";\n";
        getFunc += "  }\n";
        return cgetFunc + getFunc;
    }
    
    public String genJavaCompareTo(String fname) {
        return "    ret = "+fname+".compareTo(peer."+fname+");\n";
    }
    
    public String genJavaEquals(String fname, String peer) {
        return "    ret = "+fname+".equals("+peer+");\n";
    }
    
    public String genJavaHashCode(String fname) {
        return "    ret = "+fname+".hashCode();\n";
    }
}
