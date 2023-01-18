package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import com.google.gson.Gson;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DiskCacheBlockReader implements IBlockReader, OnLRUCacheRemoveListener<KeyBlockIdCacheKey, Path> {
    private static final Logger LOG = LoggerFactory.getLogger(DiskCacheBlockReader.class);
    private static final String META_FILE_NAME = ".BUFFER_META.json";
    private final IBlockReader source;
    private final LRUCache<KeyBlockIdCacheKey, Path> lruCache;
    private final Path bufferDir;
    private final Path metaFilePath;
    private final int blockSize;
    private final int expires;

    public DiskCacheBlockReader(IBlockReader source, int maxCacheBlocks, Path bufferDir, int expires) throws IOException {
        this.source = source;
        this.lruCache = new LRUCache<>(maxCacheBlocks);
        this.lruCache.setOnLRUCacheRemoveListener(this);
        this.bufferDir = bufferDir;
        this.metaFilePath = Paths.get(bufferDir.toString(), META_FILE_NAME);
        this.blockSize = source.getBlockSize();
        this.expires = expires;
        Files.createDirectories(bufferDir);
        LOG.debug("constructed: {}", this);
        if (Files.exists(metaFilePath)) {
            try(Reader re = Files.newBufferedReader(metaFilePath)) {
                StringWriter sr = new StringWriter();
                IOUtils.copy(re, sr);
                loadLRUCacheMetaFromJson(sr.toString());
            }
        }
    }

    public static class PersistentData {
        public static class PersistentEntity {
            public KeyBlockIdCacheKey key;
            public String value;

            public PersistentEntity(KeyBlockIdCacheKey key, String value) {
                this.key = key;
                this.value = value;
            }

            public static PersistentEntity fromMapEntry(Map.Entry<KeyBlockIdCacheKey, Path> e) {
                return new PersistentEntity(e.getKey(), e.getValue().toString());
            }
        }
        // 块数据条目
        public List<PersistentEntity> items;
        // 当前缓存块大小
        public int blockSize;

        public PersistentData(List<PersistentEntity> items, int blockSize) {
            this.items = items;
            this.blockSize = blockSize;
        }

        public static PersistentData fromMap(Map<KeyBlockIdCacheKey, Path> m, int blockSize) {
            List<PersistentEntity> items = m.entrySet()
                    .stream()
                    .map(PersistentEntity::fromMapEntry)
                    .collect(Collectors.toList());
            return new PersistentData(items, blockSize);
        }

        public LRUCache<KeyBlockIdCacheKey, Path> toLRUCache(int maxCacheBlocks) {
            LRUCache<KeyBlockIdCacheKey, Path> cache = new LRUCache<>(maxCacheBlocks);
            for (PersistentEntity item: items) {
                cache.put(item.key, Paths.get(item.value));
            }
            return cache;
        }

        public void addToMap(Map<KeyBlockIdCacheKey, Path> m) {
            for (PersistentEntity item: items) {
                m.put(item.key, Paths.get(item.value));
            }
        }
    }


    public String saveLRUCacheMetaToJson() {
        Gson gson = new Gson();
        return gson.toJson(PersistentData.fromMap(lruCache, blockSize));
    }

    public void loadLRUCacheMetaFromJson(String json) throws IOException {
        Gson gson = new Gson();
        PersistentData data = gson.fromJson(json, PersistentData.class);
        if (data.blockSize != blockSize) {
            // 块大小被改变了，缓存全部失效，需要清除缓存
            LOG.info("BlockSize was changed, clear all cached block.");
            LOG.info("old blockSize: {}, new blockSize: {}", data.blockSize, blockSize);
            FileDeleteStrategy.FORCE.delete(bufferDir.toFile());
            Files.createDirectories(bufferDir);
            return;
        }
        data.addToMap(lruCache);
    }


    private byte[] readFile(Path path) throws IOException {
        LOG.debug("read file: {}", path);

        int bs = getBlockSize();
        try(InputStream is = Files.newInputStream(path)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream(bs);
            IOUtils.copy(is, os);
            return os.toByteArray();
        }
    }
    private void writeFile(Path path, byte[] data) throws IOException {
        LOG.debug("write file: {}", path);
        Path parentPath = path.getParent();
        if(parentPath.toFile().mkdirs()) {
            LOG.debug("mkdirs: {}", parentPath);
        }
        int bs = getBlockSize();
        if (data.length > bs) throw new IOException("Cache block size error!!!");
        try(OutputStream f = Files.newOutputStream(path)) {
            f.write(data);
        }
        // 刷新元数据存储
        saveBlockCacheMetaFile();
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) {
        LOG.debug("readBlockId: {}", blockId);
        KeyBlockIdCacheKey kbck = KeyBlockIdCacheKey.get(key, blockId);

        IOException exception = null;
        for(int i=0;i<3;i++) {
            try {
                if (expires != 0) {
                    if (lruCache.containsKey(kbck)) {
                        Path blockFile = lruCache.get(kbck);
                        long now = Instant.now().getEpochSecond();
                        long lastModifiedTime = Files.getLastModifiedTime(blockFile).toInstant().getEpochSecond();
                        int duration = (int)(now - lastModifiedTime);
                        if (duration > expires) {
                            // 过期了，删除缓存块
                            Files.deleteIfExists(blockFile);
                            lruCache.remove(kbck);
                            // 需要刷新缓存元数据文件
                            saveBlockCacheMetaFile();
                        }else {
                            // 没过期，可以直接返回
                            return readFile(blockFile);
                        }
                    }
                }

                // 可能没有缓存, 缓存过期, 缓存有效期为0，直接穿透至下一层数据源获取数据
                Path cachedBlockFile = Paths.get(bufferDir.toString(), key, String.format("%d.blk", blockId));
                byte[] blockData = source.readBlock(key, blockId);
                writeFile(cachedBlockFile, blockData);
                lruCache.put(kbck, cachedBlockFile);
                return blockData;
            } catch (IOException e) {
                exception = e;
                // 缓存可能有问题，删了
                LOG.info("delete cache: {}", kbck);
                lruCache.remove(kbck);
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void onRemove(Map.Entry<KeyBlockIdCacheKey, Path> entry) {
        LOG.debug("delete file: {}", entry.getValue());
        boolean success = entry.getValue().toFile().delete();
        if (success) {
            LOG.debug("deleted file successful: {}", entry.getValue());
        }else {
            LOG.warn("deleted file failed: {}", entry.getValue());
        }
    }

    private void saveBlockCacheMetaFile() throws IOException{
        String json = saveLRUCacheMetaToJson();
        try(Writer wr = Files.newBufferedWriter(metaFilePath)) {
            wr.write(json);
            LOG.debug("Disk cache meta file has been saved in: {}", metaFilePath);
        }
    }

    @Override
    public void close() throws IOException {
        saveBlockCacheMetaFile();
        LOG.debug("Disk cache has been closed");
    }
}
