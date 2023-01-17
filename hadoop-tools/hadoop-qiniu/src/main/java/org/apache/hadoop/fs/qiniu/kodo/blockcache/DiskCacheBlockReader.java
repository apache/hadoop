package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.gson.Gson;

public class DiskCacheBlockReader implements IBlockReader, OnLRUCacheRemoveListener<KeyBlockIdCacheKey, Path> {
    private static final Logger LOG = LoggerFactory.getLogger(DiskCacheBlockReader.class);
    private static final String META_FILE_NAME = "BUFFER_META.json";
    private final IBlockReader source;
    private final LRUCache<KeyBlockIdCacheKey, Path> lruCache;
    private final Path bufferDir;
    private final Path metaFilePath;
    private final int blockSize;

    public DiskCacheBlockReader(IBlockReader source, int maxCacheBlocks, Path bufferDir) throws IOException {
        this.source = source;
        this.lruCache = new LRUCache<>(maxCacheBlocks);
        this.lruCache.setOnLRUCacheRemoveListener(this);
        this.bufferDir = bufferDir;
        this.metaFilePath = Paths.get(bufferDir.toString(), META_FILE_NAME);
        this.blockSize = source.getBlockSize();
        Files.createDirectories(bufferDir);
        LOG.debug("constructed: {}", this);
        if (metaFilePath.toFile().exists()) {
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
        public List<PersistentEntity> items;

        public PersistentData(List<PersistentEntity> items) {
            this.items = items;
        }

        public static PersistentData fromMap(Map<KeyBlockIdCacheKey, Path> m) {
            List<PersistentEntity> items = m.entrySet()
                    .stream()
                    .map(PersistentEntity::fromMapEntry)
                    .collect(Collectors.toList());
            return new PersistentData(items);
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
        return gson.toJson(PersistentData.fromMap(lruCache));
    }

    public void loadLRUCacheMetaFromJson(String json) {
        Gson gson = new Gson();
        gson.fromJson(json, PersistentData.class).addToMap(lruCache);
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
                if (lruCache.containsKey(kbck)) {
                    Path blockFile = lruCache.get(kbck);
                    return readFile(blockFile);
                }

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

    @Override
    public void close() throws IOException {
        String json = saveLRUCacheMetaToJson();
        try(Writer wr = Files.newBufferedWriter(metaFilePath)) {
            wr.write(json);
        }
    }
}
