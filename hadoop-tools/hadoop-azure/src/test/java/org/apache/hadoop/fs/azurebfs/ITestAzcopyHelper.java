package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ITestAzcopyHelper {

    public void createFileUsingAzcopy(String pathFromContainerRoot) throws IOException, InterruptedException {
        // Find the hadoop-azure directory from the current working directory
        File currentDir = new File(System.getProperty("user.dir"));
        File hadoopAzureDir;
        if (!currentDir.isDirectory() && !currentDir.getName().equals("hadoop-azure")) {
            hadoopAzureDir = findHadoopAzureDir(currentDir);
            if (hadoopAzureDir == null) {
                throw new FileNotFoundException("hadoop-azure directory not found");
            }
        } else {
            hadoopAzureDir = currentDir;
        }

        // Check if azcopy directory is present in the hadoop-azure directory, create it if it doesn't exist
        String azcopyDirPath = hadoopAzureDir.getAbsolutePath() + "/azcopy";
        File azcopyDir = new File(azcopyDirPath);
        if (!azcopyDir.exists()) {
            azcopyDir.mkdir();
        }

        // Check if azcopy is present in the azcopy directory
        String azcopyPath = azcopyDirPath + "/azcopy";
        File azcopyFile = new File(azcopyPath);
        if (!azcopyFile.exists()) {
            // If azcopy is not present, download and extract it
            String downloadUrl = "https://aka.ms/downloadazcopy-v10-linux";
            String downloadCmd = "wget " + downloadUrl + " -O azcopy.tar.gz && tar -xf azcopy.tar.gz -C " + azcopyDirPath + " && rm azcopy.tar.gz";
            String[] downloadCmdArr = {"bash", "-c", downloadCmd};
            Process downloadProcess = Runtime.getRuntime().exec(downloadCmdArr);
            downloadProcess.waitFor();

            // Set the execute permission on the azcopy executable
            String chmodCmd = "chmod +x " + azcopyPath;
            String[] chmodCmdArr = {"bash", "-c", chmodCmd};
            Process chmodProcess = Runtime.getRuntime().exec(chmodCmdArr);
            chmodProcess.waitFor();
        }

        // Change working directory to the hadoop-azure directory
        System.setProperty("user.dir", hadoopAzureDir.getAbsolutePath());
    }

    private File findHadoopAzureDir(File dir) {
        if (dir == null) {
            return null;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            return null;
        }
        for (File file : files) {
            if (file.isDirectory() && file.getName().equals("hadoop-azure")) {
                return file;
            } else {
                File hadoopAzureDir = findHadoopAzureDir(file);
                if (hadoopAzureDir != null) {
                    return hadoopAzureDir;
                }
            }
        }
        return null;
    }

    @Test
    public void testAzcopyDownload() throws IOException, InterruptedException {
        createFileUsingAzcopy("abcd");
    }
}