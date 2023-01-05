package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.storage.Region;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class QiniuKodoFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystem.class);

    private URI uri;
    private String bucket;
    private String username;
    private Path workingDir;

    private QiniuKodoClient kodoClient;

    private ExecutorService boundedThreadPool;

    private ExecutorService boundedCopyThreadPool;

    private QiniuKodoFsConfig fsConfig;
    @Override
    public void close() throws IOException {
        try {
            boundedThreadPool.shutdown();
            boundedCopyThreadPool.shutdown();
        } finally {
            super.close();
        }
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        this.fsConfig = new QiniuKodoFsConfig(getConf());
        bucket = name.getHost();
        LOG.debug("== bucket:" + bucket);

        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        LOG.debug("== uri:" + uri);

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        LOG.debug("== username:" + username);

        workingDir = new Path("/user", username).makeQualified(uri, null);
        LOG.debug("== workingDir:" + workingDir);

        this.boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
                10, 128, 60, TimeUnit.SECONDS,
                "kodo-transfer-shared");

        this.boundedCopyThreadPool = BlockingThreadPoolExecutorService.newInstance(
                25, 10485760, 60L,
                TimeUnit.SECONDS, "kodo-copy-unbounded");

        Auth auth = fsConfig.createAuth();
        kodoClient = new QiniuKodoClient(auth, bucket);

        mkdir(workingDir);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * 打开一个文件，返回一个可以被读取的输入流
     */
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.debug("== open, path:" + path);

        final FileStatus fileStatus = getFileStatus(path);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + path +
                    " because it is a directory");
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== open, key:" + key);

        return new FSDataInputStream(
                new QiniuKodoInputStream(
                        getConf(),
                        new SemaphoredDelegatingExecutor(boundedThreadPool, 4, true),
                        4, kodoClient,
                        key,
                        fileStatus.getLen(),
                        statistics));
    }

    /**
     * 创建一个文件，返回一个可以被写入的输出流
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        LOG.debug("== create, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);
        mkdirs(path.getParent());
        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== create, key:" + key + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        return new FSDataOutputStream(new QiniuKodoOutputStream(kodoClient, key, kodoClient.getUploadToken(key, overwrite)), statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Append is not supported.");
    }


    @Override
    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        if (srcPath.isRoot()) {
            // Cannot rename root of file system
            LOG.debug("Cannot rename the root of a filesystem");
            return false;
        }
        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            return false;
        }
        FileStatus srcStatus;
        try {
            srcStatus = getFileStatus(srcPath);
        } catch (FileNotFoundException e) {
            LOG.info(e.toString());
            return false;
        }
        FileStatus dstStatus;
        try {
            dstStatus = getFileStatus(dstPath);
        } catch (FileNotFoundException fnde) {
            dstStatus = null;
        }
        if (dstStatus == null) {
            // If dst doesn't exist, check whether dst dir exists or not
            try {
                dstStatus = getFileStatus(dstPath.getParent());
            } catch (FileNotFoundException e) {
                return false;
            }
            if (!dstStatus.isDirectory()) {
                throw new IOException(String.format(
                        "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                        dstPath.getParent()));
            }
        } else {
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                return !srcStatus.isDirectory();
            } else if (dstStatus.isDirectory()) {
                // If dst is a directory
                dstPath = new Path(dstPath, srcPath.getName());
                FileStatus[] statuses;
                try {
                    statuses = listStatus(dstPath);
                } catch (FileNotFoundException fnde) {
                    statuses = null;
                }
                if (statuses != null && statuses.length > 0) {
                    // If dst exists and not a directory / not empty
                    throw new FileAlreadyExistsException(String.format(
                            "Failed to rename %s to %s, file already exists or not empty!",
                            srcPath, dstPath));
                }
            } else {
                // If dst is not a directory
                LOG.warn(String.format(
                        "Failed to rename %s to %s, file already exists!", srcPath,
                        dstPath));
                return false;
            }
        }

        boolean succeed;
        if (srcStatus.isDirectory()) {
            succeed = copyDirectory(srcPath, dstPath);
        } else {
            succeed = copyFile(srcPath, dstPath);
        }

        return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
    }

    private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        String srcKey = QiniuKodoUtils.pathToKey(workingDir, srcPath);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dstPath);
        return kodoClient.copyKey(srcKey, dstKey);
    }

    private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        String srcKey = QiniuKodoUtils.pathToKey(workingDir, srcPath);
        srcKey = QiniuKodoUtils.keyToDirKey(srcKey);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dstPath);
        dstKey = QiniuKodoUtils.keyToDirKey(dstKey);

        if (dstKey.startsWith(srcKey)) {
            LOG.warn("Cannot rename a directory to a subdirectory of self");
            return false;
        }
        return kodoClient.copyKeys(srcKey, dstKey);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.debug("== delete, path:" + path + " recursive:" + recursive);

        // 判断是否是文件
        FileStatus file;
        try {
            file = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== delete, key:" + key);

        if (file.isDirectory()) {
            return deleteDir(key, recursive);
        } else {
            return deleteFile(key);
        }
    }

    private boolean deleteFile(String fileKey) throws IOException {
        fileKey = QiniuKodoUtils.keyToFileKey(fileKey);
        LOG.debug("== delete, fileKey:" + fileKey);

        return kodoClient.deleteKey(fileKey);
    }

    private boolean deleteDir(String dirKey, boolean recursive) throws IOException {
        dirKey = QiniuKodoUtils.keyToDirKey(dirKey);
        LOG.debug("== deleteDir, dirKey:" + dirKey + " recursive:" + recursive);

        List<FileInfo> files = kodoClient.listStatus(dirKey, false);

        // 有子文件文件，但未 recursive 抛出异常
        if (files != null && files.size() > 0 && !recursive) {
            throw new IOException("file is not empty");
        }
        return kodoClient.deleteKeys(dirKey);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        LOG.debug("== listStatus, path:" + path);

        if (!path.isRoot() && getFileStatus(path.getParent()) == null) throw new FileNotFoundException(path.toString());

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.debug("== listStatus, key:" + key);

        List<FileInfo> files = kodoClient.listStatus(key, true);
        List<FileStatus> fileStatuses = new ArrayList<>();
        for (FileInfo file : files) {
            if (file != null) {
                fileStatuses.add(fileInfoToFileStatus(file));
            }
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(Path newPath) {
        LOG.debug("== setWorkingDirectory, path:" + newPath);
        workingDir = newPath;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        Stack<Path> stack = new Stack<>();
        while (path != null) {
            LOG.debug("== mkdirs, path:" + path + " permission:" + permission);
            stack.push(path);
            path = path.getParent();
        }
        while (!stack.isEmpty()) {
            mkdir(stack.pop());
        }
        return true;
    }

    /**
     * 仅仅只创建当前路径文件夹
     */
    private boolean mkdir(Path path) throws IOException {
        if (path.isRoot()) return true;
        LOG.debug("== mkdir, path:" + path);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== mkdir 01, key:" + key);

        // 1. 检查是否存在同名文件
        key = QiniuKodoUtils.keyToFileKey(key);
        LOG.debug("== mkdir file, key:" + key);

        FileInfo file = kodoClient.getFileStatus(key);
        if (file != null) {
            throw new IOException("file already exist:" + path);
        }

        // 2. 检查是否存在同名路径
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.debug("== mkdir dir, key:" + key);

        file = kodoClient.getFileStatus(key);
        if (file != null) {
            return false;
        }

        // 3. 创建路径
        return kodoClient.makeEmptyObject(key);
    }


    /**
     * 获取一个路径的文件详情
     */
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("== getFileStatus, path:" + path);

        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);
        // Root always exists
        if (key.length() == 0) return new FileStatus(0, true, 1, 0, 0, 0, null, username, username, qualifiedPath);

        // 1. key 可能是实际文件或文件夹, 也可能是中间路径

        // 先尝试查找 key
        FileInfo file = kodoClient.getFileStatus(key);

        // 能查找到, 直接返回文件信息
        if (file != null) return fileInfoToFileStatus(file);

        // 2. 非路径 key，转路径
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.debug("== getFileStatus 02, key:" + key);

        file = kodoClient.getFileStatus(key);
        if (file != null) return fileInfoToFileStatus(file);

        throw new FileNotFoundException("can't find file:" + path);
    }


    /**
     * 七牛SDK的文件信息转换为 hadoop fs 的文件信息
     */
    private FileStatus fileInfoToFileStatus(FileInfo file) {
        if (file == null) return null;

        LOG.debug("== file conv, key:" + file.key);

        long putTime = file.putTime / 10000;
        boolean isDir = QiniuKodoUtils.isKeyDir(file.key);
        return new FileStatus(
                file.fsize, // 文件大小
                isDir,
                0,
                0,
                putTime, // modification time
                putTime, // access time
                isDir?new FsPermission(0715):null,   // permission
                username,   // owner
                username,   // group
                null,   // symlink
                QiniuKodoUtils.keyToPath(uri, workingDir, file.key) // 将 key 还原成 hadoop 绝对路径
        );
    }
}
