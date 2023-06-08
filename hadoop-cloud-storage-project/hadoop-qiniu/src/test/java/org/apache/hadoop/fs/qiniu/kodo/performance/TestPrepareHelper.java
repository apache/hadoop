package org.apache.hadoop.fs.qiniu.kodo.performance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

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
        Path p1 = new Path(p, String.valueOf(count - 1));
        if (fs.exists(p1)) {
            return p;
        }
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


    public void prepareLargeDir(String path, int count) throws Exception {
        if (fs.exists(new Path(path, String.valueOf(count - 1)))) {
            return;
        }
        Path p = prePrepareLargeDir(count);

        String oldPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), p);
        String newPrefix = QiniuKodoUtils.pathToKey(fs.getWorkingDirectory(), new Path(path));
        fs.mkdirs(QiniuKodoUtils.keyToPath(fs.getUri(), fs.getWorkingDirectory(), newPrefix));
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

    public void prepareBigFile(String path, int length) throws Exception {
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

    public void prepareFileByContent(String path, int length, Function<Integer, Integer> f) throws IOException {
        Path p = new Path(path);
        FSDataOutputStream fos = fs.create(p);
        for (int i = 0; i < length; i++) {
            fos.write(f.apply(i));
        }
        fos.close();
    }
}
