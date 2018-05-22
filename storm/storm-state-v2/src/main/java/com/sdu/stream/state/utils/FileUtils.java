package com.sdu.stream.state.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

public class FileUtils {

    public static void deleteDirectoryInternal(File directory) throws IOException {
        // empty the directory first
        try {
            cleanDirectoryInternal(directory);
        }
        catch (FileNotFoundException ignored) {
            // someone concurrently deleted the directory, nothing to do for us
            return;
        }

        // delete the directory. this fails if the directory is not empty, meaning
        // if new files got concurrently created. we want to fail then.
        // if someone else deleted the empty directory concurrently, we don't mind
        // the result is the same for us, after all
        Files.deleteIfExists(directory.toPath());
    }

    public static void cleanDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                }
                throw new FileNotFoundException(directory.toString());
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectoryInternal(file);
                }
            }
        }
        else if (directory.exists()) {
            throw new IOException(directory + " is not a directory but a regular file");
        }
        else {
            // else does not exist at all
            throw new FileNotFoundException(directory.toString());
        }
    }

    private static void deleteFileOrDirectoryInternal(File file) throws IOException {
        if (file.isDirectory()) {
            // file exists and is directory
            deleteDirectoryInternal(file);
        }
        else {
            // if the file is already gone (concurrently), we don't mind
            Files.deleteIfExists(file.toPath());
        }
        // else: already deleted
    }
}
