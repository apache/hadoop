package org.apache.hadoop.fs.qiniu.kodo.blockcache;


import java.io.IOException;


public class DataFetcherBlockReader implements IBlockReader, IDataFetcher {
    private final int blockSize;
    private final IDataFetcher dataFetcher;

    public DataFetcherBlockReader(int blockSize, IDataFetcher dataFetcher) {
        this.blockSize = blockSize;
        this.dataFetcher = dataFetcher;
    }

    public DataFetcherBlockReader(int blockSize) {
        this(blockSize, null);
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) throws IOException {
        return fetch(key, (long) blockId * getBlockSize(), getBlockSize());
    }


    @Override
    public byte[] fetch(String key, long offset, int size) throws IOException {
        if (dataFetcher != null) {
            return dataFetcher.fetch(key, offset, size);
        }
        return new byte[0];
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void deleteBlocks(String key) {
        // do nothing
    }
}
