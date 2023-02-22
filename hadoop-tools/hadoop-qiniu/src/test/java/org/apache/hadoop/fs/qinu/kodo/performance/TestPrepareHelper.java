package org.apache.hadoop.fs.qinu.kodo.performance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TestPrepareHelper {
    private final Path preparePath;
    private final FileSystem fs;
    private final IQiniuKodoClient client;
    private final ExecutorService executorService;

    public TestPrepareHelper(FileSystem fs, IQiniuKodoClient client, Path preparePath, ExecutorService executorService) {
        this.fs = fs;
        this.client = client;
        this.preparePath = preparePath;
        this.executorService = executorService;
    }

    private Path prePrepareLargeDir(int count) throws Exception {
        Path p = new Path(preparePath, "large-dir-" + count);
        // 预短路判断
        Path p1 = new Path(p, String.valueOf(count - 1));
        if (fs.exists(p1)) {
            return p;
        }
        // 多线程创建
        Future<?>[] futures = new Future[count];
        for (int i = 0; i < count; i++) {
            Path file = new Path(p, String.valueOf(i));
            futures[i] = executorService.submit(() -> {
                try {
                    fs.create(file).close();
                    return null;
                } catch (IOException e) {
                    return e;
                }
            });
        }

        for (Future<?> future : futures) {
            Exception e = (Exception) future.get();
            if (e != null) {
                throw e;
            }
        }
        return p;
    }

    /**
     * 准备一个文件夹，其中包含了若干个子文件
     *
     * @param path  文件夹路径
     * @param count 子文件数目
     */
    public void prepareLargeDir(String path, int count) throws Exception {
        // 预先直接判断最后一个文件是否存在，若存在则直接完成
        if (fs.exists(new Path(path, String.valueOf(count - 1)))) {
            return;
        }
        // 开始创建大文件夹，先预创建一个prepare文件夹
        Path p = prePrepareLargeDir(count);

        // 再复制到目标文件夹，hadoop未提供copy接口，故需要依赖耦合到QiniuKodoClient层,以调用批量copy方法
        String oldPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), p);
        String newPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), new Path(path));
        // 创建目标文件夹
        fs.mkdirs(QiniuKodoUtils.keyToPath(fs.getUri(), fs.getWorkingDirectory(), newPrefix));
        // 复制key
        client.copyKeys(oldPrefix, newPrefix);
    }

    private Path prePrepareBigFile(int length) throws IOException {
        Path p = new Path(preparePath, "big-file-" + length);
        try {
            if (fs.getFileStatus(p).getLen() == length) {
                return p;
            }
        } catch (FileNotFoundException ignored) {
        }

        FSDataOutputStream fos = fs.create(p);
        for (int i = 0; i < length; i++) {
            fos.write(0);
        }
        fos.close();
        return p;
    }

    /**
     * 准备一个大文件
     *
     * @param path   文件路径
     * @param length 文件大小
     */
    public void prepareBigFile(String path, int length) throws Exception {
        boolean shouldCreate = false;
        try {
            if (fs.getFileStatus(new Path(path)).getLen() == length) {
                return;
            }
        } catch (FileNotFoundException ignored) {
        }

        Path p = prePrepareBigFile(length);
        String oldPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), p);
        String newPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), new Path(path));
        client.copyKey(oldPrefix, newPrefix);
    }
}
