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
public class JBoolean extends JType {
    
    /** Creates a new instance of JBoolean */
    public JBoolean() {
        super("bool", "boolean", "Bool", "Boolean", "toBoolean");
    }
    
    public String getSignature() {
        return "z";
    }
    
    String getJavaCompareToWrapper(String fname, String other) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("          boolean ee1 = ("+fname+".toBoolean()\n");
      sb.append("          boolean ee2 = ("+other+".toBoolean()\n");
      sb.append("          ret = (ee1 == ee2)? 0 :(ee1?-1:1);\n");
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareTo(String fname, String other) {
        return "    ret = ("+fname+" == "+other+")? 0 : ("+fname+"?1:-1);\n";
    }
    
    public String genJavaHashCode(String fname) {
        return "     ret = ("+fname+")?0:1;\n";
    }
    
    // In Binary format, boolean is written as byte. true = 1, false = 0
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if ("+l+"<1) {\n");
      sb.append("             throw new IOException(\"Boolean is exactly 1 byte. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           "+s+"++; "+l+"--;\n");
      sb.append("        }\n");
      return sb.toString();
    }
    
    // In Binary format, boolean is written as byte. true = 1, false = 0
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if (l1<1 || l2<1) {\n");
      sb.append("             throw new IOException(\"Boolean is exactly 1 byte. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           if (b1[s1] != b2[s2]) {\n");
      sb.append("             return (b1[s1]<b2[s2])? -1 : 0;\n");
      sb.append("           }\n");
      sb.append("           s1++; s2++; l1--; l2--;\n");
      sb.append("        }\n");
      return sb.toString();
    }
}
