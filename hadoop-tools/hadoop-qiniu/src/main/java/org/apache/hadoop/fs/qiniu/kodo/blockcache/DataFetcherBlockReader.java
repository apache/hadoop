package org.apache.hadoop.fs.qiniu.kodo.blockcache;



public class DataFetcherBlockReader implements IBlockReader, IDataFetcher{
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
    public byte[] readBlockById(int blockId) {
        long from = (long) blockId * getBlockSize();
        long to = from + getBlockSize();
        return fetch(from, to);
    }

    @Override
    public byte[] fetch(long from, long to) {
        if (dataFetcher != null) {
            return dataFetcher.fetch(from, to);
        }
        return new byte[0];
    }
}
