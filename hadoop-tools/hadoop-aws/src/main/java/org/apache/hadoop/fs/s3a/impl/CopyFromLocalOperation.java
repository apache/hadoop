package org.apache.hadoop.fs.s3a.impl;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * TODO list:
 * - Add abstract class + tests for LocalFS
 * - Add tests for this class
 * - Add documentation
 *  - This class
 *  - `filesystem.md`
 * - Remove remaining TODO s
 * - Clean old `innerCopyFromLocalFile` code up
 */
public class CopyFromLocalOperation extends ExecutingStoreOperation<Void>
        implements IOStatisticsSource {

    private static final Logger LOG = LoggerFactory.getLogger(
            CopyFromLocalOperation.class);

    private final CopyFromLocalOperationCallbacks callbacks;
    private final boolean delSrc;
    private final boolean overwrite;
    private final Path src;
    private final Path dst;

    public CopyFromLocalOperation(
            final StoreContext storeContext,
            boolean delSrc,
            boolean overwrite,
            Path src,
            Path dst,
            CopyFromLocalOperationCallbacks callbacks) {
        super(storeContext);
        this.callbacks = callbacks;
        this.delSrc = delSrc;
        this.overwrite = overwrite;
        this.src = src;
        this.dst = dst;
    }

    @Override
    @Retries.RetryTranslated
    public Void execute()
            throws IOException, PathExistsException {
        LOG.debug("Copying local file from {} to {}", src, dst);
        LocalFileSystem local = callbacks.getLocalFS();
        File sourceFile = local.pathToFile(src);

        checkSource(sourceFile);
        prepareDestination(dst, sourceFile.isFile(), overwrite);
        uploadSourceFromFS(local);

        if (delSrc) {
            local.delete(src, true);
        }

        return null;
    }

    private void uploadSourceFromFS(LocalFileSystem local)
            throws IOException, PathExistsException {
        RemoteIterator<LocatedFileStatus> localFiles = callbacks
                .listStatusIterator(src, true);

        // Go through all files and create UploadEntries
        List<UploadEntry> entries = new ArrayList<>();
        while (localFiles.hasNext()) {
            LocatedFileStatus status = localFiles.next();
            Path destPath = getFinalPath(status.getPath());
            // UploadEntries: have a destination path, a file size
            entries.add(new UploadEntry(
                    status.getPath(),
                    destPath,
                    status.getLen()));
        }

        if (localFiles instanceof Closeable) {
            ((Closeable) localFiles).close();
        }

        // Sort all upload entries based on size
        entries.sort(new ReverseComparator(new UploadEntry.SizeComparator()));

        int LARGEST_N_FILES = 5;
        final int sortedUploadsCount = Math.min(LARGEST_N_FILES, entries.size());
        List<UploadEntry> uploaded = new ArrayList<>();

        // Take only top most X entries and upload
        for (int uploadNo = 0; uploadNo < sortedUploadsCount; uploadNo++) {
            UploadEntry uploadEntry = entries.get(uploadNo);
            File file = local.pathToFile(uploadEntry.source);
            callbacks.copyFileFromTo(
                    file,
                    uploadEntry.source,
                    uploadEntry.destination);

            uploaded.add(uploadEntry);
        }

        // Shuffle all remaining entries and upload them
        // TODO: Should directories be handled differently?
        entries.removeAll(uploaded);
        Collections.shuffle(entries);
        for (UploadEntry uploadEntry : entries) {
            File file = local.pathToFile(uploadEntry.source);
            callbacks.copyFileFromTo(
                    file,
                    uploadEntry.source,
                    uploadEntry.destination);
        }
    }

    private void checkSource(File sourceFile)
            throws FileNotFoundException {
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("No file: " + src);
        }
    }


    private void prepareDestination(
            Path dst,
            boolean isSrcFile,
            boolean overwrite) throws PathExistsException, IOException {
        try {
            S3AFileStatus dstStatus = callbacks.getFileStatus(
                    dst,
                    false,
                    StatusProbeEnum.ALL);

            if (isSrcFile && dstStatus.isDirectory()) {
                throw new PathExistsException("Source is file and destination '" + dst + "' is directory");
            }

            if (!overwrite) {
                throw new PathExistsException(dst + " already exists");
            }
        } catch (FileNotFoundException e) {
            // no destination, all is well
        }
    }

    private Path getFinalPath(Path srcFile) throws IOException {
        // TODO: Implement this bad boy
        return null;
    }

    private static final class UploadEntry {
        private final Path source;
        private final Path destination;
        private final long size;

        private UploadEntry(Path source, Path destination, long size) {
            this.source = source;
            this.destination = destination;
            this.size = size;
        }

        static class SizeComparator implements Comparator<UploadEntry> {
            @Override
            public int compare(UploadEntry entry1, UploadEntry entry2) {
                return Long.compare(entry1.size, entry2.size);
            }
        }
    }

    public interface CopyFromLocalOperationCallbacks {
        RemoteIterator<LocatedFileStatus> listStatusIterator(
                Path f,
                boolean recursive) throws IOException;

        S3AFileStatus getFileStatus(
                final Path f,
                final boolean needEmptyDirectoryFlag,
                final Set<StatusProbeEnum> probes) throws IOException;

        LocalFileSystem getLocalFS() throws IOException;

        void copyFileFromTo(
                File file,
                Path source,
                Path destination) throws IOException;
    }
}
