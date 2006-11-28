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

/**
 *
 * @author Milind Bhandarkar
 */
public class JBuffer extends JCompType {
    
    /** Creates a new instance of JBuffer */
    public JBuffer() {
        super(" ::std::string", "java.io.ByteArrayOutputStream", "Buffer", "java.io.ByteArrayOutputStream");
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
    
    public String getSignature() {
        return "B";
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    java.io.ByteArrayOutputStream "+fname+";\n";
        }
        return ret + "        "+fname+"=a_.readBuffer(\""+tag+"\");\n";
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.writeBuffer("+fname+",\""+tag+"\");\n";
    }
    
    public String genJavaCompareTo(String fname) {
        return "";
    }
    
    public String genJavaEquals(String fname, String peer) {
        return "    ret = org.apache.hadoop.record.Utils.bufEquals("+fname+","+peer+");\n";
    }
    
    public String genJavaHashCode(String fname) {
        return "    ret = "+fname+".toString().hashCode();\n";
    }
}
