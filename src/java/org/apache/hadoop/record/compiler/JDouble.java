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
public class JDouble extends JType {
    
    /** Creates a new instance of JDouble */
    public JDouble() {
        super("double", "double", "Double", "Double", "toDouble");
    }
    
    public String getSignature() {
        return "d";
    }
    
    public String genJavaHashCode(String fname) {
        String tmp = "Double.doubleToLongBits("+fname+")";
        return "    ret = (int)("+tmp+"^("+tmp+">>>32));\n";
    }
    
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if ("+l+"<8) {\n");
      sb.append("             throw new IOException(\"Double is exactly 8 bytes. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           "+s+"+=8; "+l+"-=8;\n");
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           if (l1<8 || l2<8) {\n");
      sb.append("             throw new IOException(\"Double is exactly 8 bytes. Provided buffer is smaller.\");\n");
      sb.append("           }\n");
      sb.append("           double d1 = WritableComparator.readDouble(b1, s1);\n");
      sb.append("           double d2 = WritableComparator.readDouble(b2, s2);\n");
      sb.append("           if (d1 != d2) {\n");
      sb.append("             return ((d1-d2) < 0) ? -1 : 0;\n");
      sb.append("           }\n");
      sb.append("           s1+=8; s2+=8; l1-=8; l2-=8;\n");
      sb.append("        }\n");
      return sb.toString();
    }
}
