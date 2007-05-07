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
package org.apache.hadoop.hbase;
import org.apache.hadoop.io.*;

import java.io.*;

/*******************************************************************************
 * LabelledData is just a data pair.
 * It includes a Text label and some associated data.
 ******************************************************************************/
public class LabelledData implements Writable {
  Text label;
  BytesWritable data;

  public LabelledData() {
    this.label = new Text();
    this.data = new BytesWritable();
  }

  public LabelledData(Text label, BytesWritable data) {
    this.label = new Text(label);
    this.data = data;
  }

  public Text getLabel() {
    return label;
  }

  public BytesWritable getData() {
    return data;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    label.write(out);
    data.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    label.readFields(in);
    data.readFields(in);
  }
}
