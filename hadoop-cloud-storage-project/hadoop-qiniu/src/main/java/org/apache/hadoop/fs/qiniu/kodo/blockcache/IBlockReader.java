package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import java.io.Closeable;
import java.io.IOException;

/**
 * This interface is the block-level reader
 */
public interface IBlockReader extends Closeable {
    /**
     * Get the block size
     *
     * @return the block size
     */
    int getBlockSize();

    /**
     * Read the block data
     *
     * @param key     the key of the block data source
     * @param blockId the block id
     * @return the block data
     * @throws IOException if any error occurs
     */
    byte[] readBlock(String key, int blockId) throws IOException;

    /**
     * Delete the blocks by the key
     *
     * @param key the key of the block data source
     */
    void deleteBlocks(String key);
}
