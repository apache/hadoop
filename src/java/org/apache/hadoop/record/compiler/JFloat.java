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
public class JFloat extends JType {
    
    /** Creates a new instance of JFloat */
    public JFloat() {
        super("float", "float", "Float", "Float", "toFloat");
    }
    
    public String getSignature() {
        return "f";
    }
    
    public String genJavaHashCode(String fname) {
        return "    ret = Float.floatToIntBits("+fname+");\n";
    }
    
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if ("+l+"<4) {\n");
      sb.append("             throw new IOException(\"Float is exactly 4 bytes. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           "+s+"+=4; "+l+"-=4;\n");
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if (l1<4 || l2<4) {\n");
      sb.append("             throw new IOException(\"Float is exactly 4 bytes. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           float f1 = WritableComparator.readFloat(b1, s1);\n");
      sb.append("           float f2 = WritableComparator.readFloat(b2, s2);\n");
      sb.append("           if (f1 != f2) {\n");
      sb.append("             return ((f1-f2) < 0) ? -1 : 0;\n");
      sb.append("           }\n");
      sb.append("           s1+=4; s2+=4; l1-=4; l2-=4;\n");
      sb.append("        }\n");
      return sb.toString();
    }
}
