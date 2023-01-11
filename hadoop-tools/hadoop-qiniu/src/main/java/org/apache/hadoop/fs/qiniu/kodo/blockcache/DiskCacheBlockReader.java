package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class DiskCacheBlockReader implements IBlockReader, OnLRUCacheRemoveListener<Integer, Path> {
    private static final Logger LOG = LoggerFactory.getLogger(DiskCacheBlockReader.class);
    private final IBlockReader source;
    private final LRUCache<Integer, Path> lruCache;
    private final Path bufferDir;

    public DiskCacheBlockReader(IBlockReader source, int maxCacheBlocks, Path bufferDir) throws IOException {
        this.source = source;
        this.lruCache = new LRUCache<>(maxCacheBlocks);
        this.lruCache.setOnLRUCacheRemoveListener(this);
        this.bufferDir = bufferDir;
        Files.createDirectories(bufferDir);
        LOG.debug("constructed: {}", this);
    }

    private byte[] readFile(Path path) throws IOException {
        LOG.debug("read file: {}", path);

        File file = path.toFile();

        int bs = getBlockSize();
        try(InputStream f = new BufferedInputStream(new FileInputStream(file))) {
            int fileLength = (int) file.length();
            byte[] result;
            if (fileLength < bs) {
                result = new byte[fileLength];
            } else {
                result = new byte[bs];
            }
            if(f.read(result) != -1) throw new IOException("Cache file " + file + "error!!!");
            return result;
        }
    }
    private void writeFile(Path path, byte[] data) throws IOException {
        LOG.debug("write file: {}", path);

        int bs = getBlockSize();
        if (data.length > bs) throw new IOException("Cache block size error!!!");
        File file = path.toFile();
        try(OutputStream f = new BufferedOutputStream(new FileOutputStream(file))) {
            f.write(data);
        }
    }
    @Override
    public int getBlockSize() {
        int blkSize = source.getBlockSize();
        LOG.debug("blockSize: {}", blkSize);
        return blkSize;
    }

    @Override
    public byte[] readBlockById(int blockId) {
        LOG.debug("readBlockId: {}", blockId);
        try {
            if (lruCache.containsKey(blockId)) {
                Path blockFile = lruCache.get(blockId);
                return readFile(blockFile);
            }

            Path cachedBlockFile = Paths.get(bufferDir.toString(), String.format("%d.blk", blockId));
            byte[] blockData = source.readBlockById(blockId);
            writeFile(cachedBlockFile, blockData);
            lruCache.put(blockId, cachedBlockFile);
            return blockData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onRemove(Map.Entry<Integer, Path> entry) {
        LOG.debug("delete file: {}", entry.getValue());
        boolean success = entry.getValue().toFile().delete();
        if (success) {
            LOG.debug("deleted file successful: {}", entry.getValue());
        }else {
            LOG.warn("deleted file failed: {}", entry.getValue());
        }
    }
}
