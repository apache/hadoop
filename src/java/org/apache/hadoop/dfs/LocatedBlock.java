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
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;

import java.io.*;

/****************************************************
 * A LocatedBlock is a pair of Block, DatanodeInfo[]
 * objects.  It tells where to find a Block.
 * 
 * @author Michael Cafarella
 ****************************************************/
class LocatedBlock implements Writable {

    static {                                      // register a ctor
      WritableFactories.setFactory
        (LocatedBlock.class,
         new WritableFactory() {
           public Writable newInstance() { return new LocatedBlock(); }
         });
    }

    Block b;
    DatanodeInfo locs[];

    /**
     */
    public LocatedBlock() {
        this.b = new Block();
        this.locs = new DatanodeInfo[0];
    }

    /**
     */
    public LocatedBlock(Block b, DatanodeInfo[] locs) {
        this.b = b;
        this.locs = locs;
    }

    /**
     */
    public Block getBlock() {
        return b;
    }

    /**
     */
    DatanodeInfo[] getLocations() {
        return locs;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        b.write(out);
        out.writeInt(locs.length);
        for (int i = 0; i < locs.length; i++) {
            locs[i].write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.b = new Block();
        b.readFields(in);
        int count = in.readInt();
        this.locs = new DatanodeInfo[count];
        for (int i = 0; i < locs.length; i++) {
            locs[i] = new DatanodeInfo();
            locs[i].readFields(in);
        }
    }
}
