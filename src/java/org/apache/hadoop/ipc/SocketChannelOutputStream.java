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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/* ------------------------------------------------------------------------------- */
/** 
 * Blocking output stream on non-blocking SocketChannel.  Makes the 
 * assumption that writes will rarely need to block.
 * All writes flush to the channel, and no additional buffering is done.
 */
class SocketChannelOutputStream extends OutputStream {    
    
    ByteBuffer buffer;
    ByteBuffer flush;
    SocketChannel channel;
    Selector selector;
    
    /* ------------------------------------------------------------------------------- */
    /** Constructor.
     * 
     */
    public SocketChannelOutputStream(SocketChannel channel)
    {
        this.channel = channel;
        buffer = ByteBuffer.allocate(8); // only for small writes
    }

    /* ------------------------------------------------------------------------------- */
    /*
     * @see java.io.OutputStream#write(int)
     */
    public void write(int b) throws IOException
    {
        buffer.clear();
        buffer.put((byte)b);
        buffer.flip();
        flush = buffer;
        flushBuffer();
    }

    
    /* ------------------------------------------------------------------------------- */
    /*
     * @see java.io.OutputStream#close()
     */
    public void close() throws IOException
    {
        channel.close();
    }

    /* ------------------------------------------------------------------------------- */
    /*
     * @see java.io.OutputStream#flush()
     */
    public void flush() throws IOException
    {
    }

    /* ------------------------------------------------------------------------------- */
    /*
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    public void write(byte[] buf, int offset, int length) throws IOException
    {
        flush = ByteBuffer.wrap(buf,offset,length);
        flushBuffer();
    }

    /* ------------------------------------------------------------------------------- */
    /*
     * @see java.io.OutputStream#write(byte[])
     */
    public void write(byte[] buf) throws IOException
    {
        flush = ByteBuffer.wrap(buf);
        flushBuffer();
    }


    /* ------------------------------------------------------------------------------- */
    private void flushBuffer() throws IOException
    {
        while (flush.hasRemaining())
        {
            int len = channel.write(flush);
            if (len < 0)
                throw new IOException("EOF");
            if (len == 0)
            {
                // write channel full.  Try letting other threads have a go.
                Thread.yield();
                len = channel.write(flush);
                if (len < 0)
                    throw new IOException("EOF");
                if (len == 0)
                {
                    // still full.  need to  block until it is writable.
                    if (selector==null)
                     {
                            selector = Selector.open();
                            channel.register(selector, SelectionKey.OP_WRITE);
                     }

                     selector.select();
                }
            }
        }
        flush = null;
    }

    /* ------------------------------------------------------------------------------- */
    public void destroy()
    {
        if (selector != null)
        {
            try{ selector.close();}
            catch(IOException e){}
            selector = null;
            buffer = null;
            flush = null;
            channel = null;
        }
    }
}
