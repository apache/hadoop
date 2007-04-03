/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/*******************************************************************************
 * HScannerInterface iterates through a set of rows.  It's implemented by several classes.
 ******************************************************************************/
public interface HScannerInterface {
  public boolean next(HStoreKey key, TreeMap<Text, byte[]> results) throws IOException;
  public void close() throws IOException;
}
