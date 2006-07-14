/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.hadoop.streaming;

import java.io.*;

/** A minimal Java implementation of /usr/bin/tr.
    Used to test the usage of external applications without adding
    platform-specific dependencies.
*/
public class TrApp
{

  public TrApp(char find, char replace)
  {
    this.find = find;
    this.replace = replace;
  }

  public void go() throws IOException
  {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    while ((line = in.readLine()) != null) {
        String out = line.replace(find, replace);
        System.out.println(out);
    }
  }

  public static void main(String[] args) throws IOException
  {
    args[0] = CUnescape(args[0]);
    args[1] = CUnescape(args[1]);
    TrApp app = new TrApp(args[0].charAt(0), args[1].charAt(0));
    app.go();
  }

  public static String CUnescape(String s)
  {
    if(s.equals("\\n")) {
      return "\n";
    } else {
      return s;
    }
  }
  char find;
  char replace;

}
