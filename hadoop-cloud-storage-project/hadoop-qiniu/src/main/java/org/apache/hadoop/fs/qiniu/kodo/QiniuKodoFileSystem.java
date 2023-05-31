package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.common.QiniuException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoCachedClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoFileInfo;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.download.EmptyInputStream;
import org.apache.hadoop.fs.qiniu.kodo.download.QiniuKodoInputStream;
import org.apache.hadoop.fs.qiniu.kodo.download.blockreader.QiniuKodoGeneralBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.download.blockreader.QiniuKodoRandomBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.upload.QiniuKodoOutputStream;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QiniuKodoFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystem.class);

    private URI uri;
    private String username;
    private Path workingDir;

    private IQiniuKodoClient kodoClient;

    private QiniuKodoFsConfig fsConfig;
    private QiniuKodoGeneralBlockReader generalblockReader;
    private QiniuKodoRandomBlockReader randomBlockReader;
    private ExecutorService uploadExecutorService;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        LOG.debug("initialize QiniuKodoFileSystem with uri: {}", name);
        setConf(conf);
        this.fsConfig = new QiniuKodoFsConfig(getConf());
        setLog4jConfig(fsConfig);

        String bucket = name.getHost();
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        this.username = UserGroupInformation.getCurrentUser().getShortUserName();
        this.workingDir = new Path("/user", username).makeQualified(uri, null);

        if (fsConfig.client.cache.enable) {
            this.kodoClient = buildKodoClient(bucket, fsConfig);
            this.kodoClient = new QiniuKodoCachedClient(this.kodoClient, fsConfig.client.cache.maxCapacity);
        } else {
            this.kodoClient = buildKodoClient(bucket, fsConfig);
        }

        this.generalblockReader = new QiniuKodoGeneralBlockReader(fsConfig, kodoClient);
        this.randomBlockReader = new QiniuKodoRandomBlockReader(
                kodoClient,
                fsConfig.download.random.blockSize,
                fsConfig.download.random.maxBlocks
        );
    }

    protected IQiniuKodoClient buildKodoClient(String bucket, QiniuKodoFsConfig fsConfig) throws QiniuException, AuthorizationException {
        return new QiniuKodoClient(bucket, fsConfig, statistics);
    }

    public IQiniuKodoClient getKodoClient() {
        return kodoClient;
    }

    protected static void setLog4jConfig(QiniuKodoFsConfig fsConfig) {
        org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
        if (fsConfig.logger.level != null) {
            rootLogger.setLevel(Level.toLevel(fsConfig.logger.level));
        }
    }

    private volatile boolean makeSureWorkdirCreatedFlag = false;

    /**
     * 工作目录为相对路径使用的目录，其必须得存在，故需要检查是否被创建
     * 需要在创建文件，创建文件夹前进行检查
     * 需要在工作目录被改变的时候重新检查
     */
    private synchronized void makeSureWorkdirCreated(Path path) throws IOException {
        if (makeSureWorkdirCreatedFlag) return;
        if (!path.makeQualified(uri, workingDir).toString().startsWith(
                workingDir.makeQualified(uri, workingDir).toString())) {
            return;
        }
        mkdirs(workingDir);
        makeSureWorkdirCreatedFlag = true;
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
        IOException fnfeDir = new FileNotFoundException("Can't open " + path +
                " because it is a directory");

        LOG.debug("open, path:" + path);

        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);

        // root
        if (key.length() == 0) throw fnfeDir;

        FileStatus fileStatus = getFileStatus(qualifiedPath);
        if (fileStatus.isDirectory()) throw fnfeDir;

        long len = fileStatus.getLen();
        // 空文件内容
        if (len == 0) {
            return new FSDataInputStream(new EmptyInputStream());
        }

        return new FSDataInputStream(
                new QiniuKodoInputStream(
                        key,
                        fsConfig.download.random.enable,
                        generalblockReader,
                        randomBlockReader,
                        len, statistics
                )
        );
    }

    @Override
    public void close() throws IOException {
        super.close();
        generalblockReader.close();
    }

    private synchronized ExecutorService getUploadExecutorService() {
        if (uploadExecutorService == null) {
            uploadExecutorService = Executors.newFixedThreadPool(fsConfig.upload.maxConcurrentUploadFiles);
        }
        return uploadExecutorService;
    }

    private void deleteKeyBlocks(String key) {
        generalblockReader.deleteBlocks(key);
        randomBlockReader.deleteBlocks(key);
    }


    /**
     * 创建一个文件，返回一个可以被写入的输出流
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        IOException faee = new FileAlreadyExistsException("Can't create file " + path +
                " because it is a directory");
        LOG.debug("create, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        if (path.isRoot()) throw faee;

        try {
            FileStatus fileStatus = getFileStatus(path);
            // 文件已存在, 如果是文件夹则抛出异常
            if (fileStatus.isDirectory()) {
                throw faee;
            } else {
                // 文件已存在，如果不能覆盖则抛出异常
                if (!overwrite) {
                    throw new FileAlreadyExistsException("File already exists: " + path);
                }
            }
        } catch (FileNotFoundException e) {
            // ignore
            // 文件不存在，可以被创建
        }
        makeSureWorkdirCreated(path);
        mkdirs(path.getParent());
        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        if (overwrite) {
            deleteKeyBlocks(key);
        }

        return new FSDataOutputStream(
                new QiniuKodoOutputStream(
                        kodoClient,
                        key,
                        overwrite,
                        fsConfig.upload.bufferSize,
                        getUploadExecutorService()
                ),
                statistics
        );
    }

    /**
     * 创建一个文件，返回一个可以被写入的输出流，该创建文件的方法不会递归创建父目录
     *
     * @param path        the file name to open
     * @param permission  file permission
     * @param flags       {@link CreateFlag}s to use for this stream.
     * @param bufferSize  the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize   block size
     * @param progress    the progress reporter
     * @return a new FSDataOutputStream
     * @throws IOException IO Exception
     */
    @Override
    public FSDataOutputStream createNonRecursive(
            Path path, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        boolean overwrite = flags.contains(CreateFlag.OVERWRITE);

        IOException faee = new FileAlreadyExistsException("Can't create file " + path +
                " because it is a directory");
        LOG.debug("createNonRecursive, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        try {
            FileStatus fileStatus = getFileStatus(path);
            // 文件已存在, 如果是文件夹则抛出异常
            if (fileStatus.isDirectory()) {
                throw faee;
            } else {
                // 文件已存在，如果不能覆盖则抛出异常
                if (!overwrite) {
                    throw new FileAlreadyExistsException("File already exists: " + path);
                }
            }
        } catch (FileNotFoundException e) {
            // ignore
            // 文件不存在，可以被创建
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        if (overwrite) {
            deleteKeyBlocks(key);
        }

        return new FSDataOutputStream(
                new QiniuKodoOutputStream(
                        kodoClient,
                        key,
                        overwrite,
                        fsConfig.upload.bufferSize,
                        getUploadExecutorService()
                ),
                statistics
        );
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
        makeSureWorkdirCreated(dstPath);

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
        } catch (FileNotFoundException fnde) {
            srcStatus = null;
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
                throw new FileAlreadyExistsException(String.format(
                        "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                        dstPath.getParent()));
            }
        } else {
            assert srcStatus != null;
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
                throw new FileAlreadyExistsException(String.format(
                        "Failed to rename %s to %s, file already exists!",
                        srcPath, dstPath));
            }
        }

        if (srcStatus != null) {
            boolean succeed;
            if (srcStatus.isDirectory()) {
                succeed = copyDirectory(srcPath, dstPath);
            } else {
                succeed = copyFile(srcPath, dstPath);
            }
            return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
        } else {
            throw new FileNotFoundException(srcPath.toString());
        }
    }

    private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        String srcKey = QiniuKodoUtils.pathToKey(workingDir, srcPath);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dstPath);
        kodoClient.copyKey(srcKey, dstKey);
        return true;
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
        kodoClient.copyKeys(srcKey, dstKey);
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.debug("delete, path:" + path + " recursive:" + recursive);

        // 判断是否是文件
        FileStatus file;
        try {
            file = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("delete, key:" + key);

        if (file.isDirectory()) {
            deleteDir(key, recursive);
        } else {
            deleteFile(key);
        }
        return true;
    }

    private void deleteFile(String fileKey) throws IOException {
        fileKey = QiniuKodoUtils.keyToFileKey(fileKey);
        LOG.debug("delete, fileKey:" + fileKey);
        deleteKeyBlocks(fileKey);
        kodoClient.deleteKey(fileKey);
    }

    private void deleteDir(String dirKey, boolean recursive) throws IOException {
        dirKey = QiniuKodoUtils.keyToDirKey(dirKey);
        LOG.debug("deleteDir, dirKey:" + dirKey + " recursive:" + recursive);
        if (kodoClient.listNStatus(dirKey, 2).size() > 1 && !recursive) {
            // 若非递归且不止一个，那么抛异常
            throw new IOException("Dir is not empty");
        }
        deleteKeyBlocks(dirKey);
        kodoClient.deleteKeys(dirKey);
    }


    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return RemoteIterators.toArray(listStatusIterator(path), new FileStatus[0]);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path path) throws IOException {
        FileStatus status = getFileStatus(path);
        if (status.isFile()) {
            return RemoteIterators.remoteIteratorFromSingleton(status);
        }

        final String key = QiniuKodoUtils.keyToDirKey(QiniuKodoUtils.pathToKey(workingDir, path));
        return RemoteIterators.mappingRemoteIterator(
                kodoClient.listStatusIterator(key, true),
                this::fileInfoToFileStatus
        );
    }

    @Override
    public void setWorkingDirectory(Path newPath) {
        workingDir = newPath;
        makeSureWorkdirCreatedFlag = false;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    /**
     * 递归地创建文件夹
     */
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        List<Path> successPaths = new ArrayList<>();
        // 如果该文件夹发现已存在，那么直接返回结果
        try {
            while (path != null) {
                boolean success = mkdir(path);
                if (!success) {
                    break;
                }
                successPaths.add(path);
                path = path.getParent();
            }
            return true;
        } catch (FileAlreadyExistsException e) {
            // 回滚删除已创建成功的中间文件
            for (Path p : successPaths) {
                delete(p, false);
            }
            throw e;
        }

    }

    /**
     * 仅仅只创建当前路径文件夹
     */
    private boolean mkdir(Path path) throws IOException {
        LOG.debug("mkdir, path:" + path);

        // 根目录
        if (path.isRoot()) {
            return false;
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        String dirKey = QiniuKodoUtils.keyToDirKey(key);
        String fileKey = QiniuKodoUtils.keyToFileKey(key);

        // 找到文件夹，返回
        if (kodoClient.exists(dirKey)) {
            return false;
        }

        // 找到文件，抛异常
        if (kodoClient.exists(fileKey)) {
            throw new FileAlreadyExistsException(path.toString());
        }

        // 啥都找不到，创建文件夹
        kodoClient.makeEmptyObject(dirKey);
        return true;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);

        // Root always exists
        if (key.length() == 0) return true;

        // 1. key 可能是实际文件或文件夹, 也可能是中间路径

        // 先尝试查找 key
        if (kodoClient.exists(key)) return true;

        // 2. 有可能是文件夹路径但是不存在末尾/
        // 添加尾部/后再次获取
        String dirKey = QiniuKodoUtils.keyToDirKey(key);

        // 找不到表示文件夹的空对象，故只能列举是否有该前缀的对象
        // 列举结果为null，则真的不存在
        return kodoClient.listOneStatus(dirKey) != null;
    }

    /**
     * 获取一个路径的文件详情
     */
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("getFileStatus, path:" + path);

        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);


        // Root always exists
        if (key.length() == 0) {
            return new FileStatus(0, true, 1, 0, 0, 0, null, username, username, qualifiedPath);
        }

        // 1. key 可能是实际文件或文件夹, 也可能是中间路径

        // 先尝试查找 key
        QiniuKodoFileInfo file = kodoClient.getFileStatus(key);

        // 能查找到, 直接返回文件信息
        if (file != null) {
            return fileInfoToFileStatus(file);
        }

        // 2. 有可能是文件夹路径但是不存在末尾/
        // 添加尾部/后再次获取
        String newKey = QiniuKodoUtils.keyToDirKey(key);

        // 找不到表示文件夹的空对象，故只能列举是否有该前缀的对象
        file = kodoClient.listOneStatus(newKey);
        if (file == null) {
            throw new FileNotFoundException("can't find file:" + path);
        }

        // 是文件夹本身
        if (file.key.equals(newKey)) {
            return fileInfoToFileStatus(file);
        }

        // 是文件夹前缀
        QiniuKodoFileInfo newDir = new QiniuKodoFileInfo(newKey, 0, 0);
        kodoClient.makeEmptyObject(newKey);
        return fileInfoToFileStatus(newDir);
    }


    /**
     * 七牛SDK的文件信息转换为 hadoop fs 的文件信息
     */
    private FileStatus fileInfoToFileStatus(QiniuKodoFileInfo file) {
        if (file == null) return null;

        LOG.debug("file stat, key:" + file.key);

        long putTime = file.putTime;
        boolean isDir = QiniuKodoUtils.isKeyDir(file.key);
        return new FileStatus(
                file.size, // 文件大小
                isDir,
                0,
                fsConfig.download.blockSize,
                putTime, // modification time
                putTime, // access time
                FsPermission.createImmutable(
                        isDir
                                ? (short) 0715 // rwxrwxrwx
                                : (short) 0666 // rw-rw-rw-
                ),   // permission
                username,   // owner
                username,   // group
                null,   // symlink
                QiniuKodoUtils.keyToPath(uri, workingDir, file.key) // 将 key 还原成 hadoop 绝对路径
        );
    }
}
