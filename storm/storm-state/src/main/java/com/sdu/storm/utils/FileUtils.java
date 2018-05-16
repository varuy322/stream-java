package com.sdu.storm.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static com.sdu.storm.utils.Preconditions.checkNotNull;

public class FileUtils {

    /** Global lock to prevent concurrent directory deletes under Windows. */
    private static final Object WINDOWS_DELETE_LOCK = new Object();

    /** The alphabet to construct the random part of the filename from. */
    private static final char[] ALPHABET =
            { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f' };

    /** The length of the random part of the filename. */
    private static final int RANDOM_FILE_NAME_LENGTH = 12;

    // ------------------------------------------------------------------------

    /**
     * Constructs a random filename with the given prefix and
     * a random part generated from hex characters.
     *
     * @param prefix
     *        the prefix to the filename to be constructed
     * @return the generated random filename with the given prefix
     */
    public static String getRandomFilename(final String prefix) {
        final Random rnd = new Random();
        final StringBuilder stringBuilder = new StringBuilder(prefix);

        for (int i = 0; i < RANDOM_FILE_NAME_LENGTH; i++) {
            stringBuilder.append(ALPHABET[rnd.nextInt(ALPHABET.length)]);
        }

        return stringBuilder.toString();
    }

    // ------------------------------------------------------------------------
    //  Simple reading and writing of files
    // ------------------------------------------------------------------------

    public static String readFile(File file, String charsetName) throws IOException {
        byte[] bytes = Files.readAllBytes(file.toPath());
        return new String(bytes, charsetName);
    }

    public static String readFileUtf8(File file) throws IOException {
        return readFile(file, "UTF-8");
    }

    public static void writeFile(File file, String contents, String encoding) throws IOException {
        byte[] bytes = contents.getBytes(encoding);
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
    }

    public static void writeFileUtf8(File file, String contents) throws IOException {
        writeFile(file, contents, "UTF-8");
    }

    // ------------------------------------------------------------------------
    //  Deleting directories on standard File Systems
    // ------------------------------------------------------------------------

    /**
     * Removes the given file or directory recursively.
     *
     * <p>If the file or directory does not exist, this does not throw an exception, but simply does nothing.
     * It considers the fact that a file-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param file The file or directory to delete.
     *
     * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
     *                     due to missing access/write permissions.
     */
    public static void deleteFileOrDirectory(File file) throws IOException {
        checkNotNull(file, "file");

        guardIfWindows(FileUtils::deleteFileOrDirectoryInternal, file);
    }

    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does nothing.
     * It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the given file is not a directory, or if the directory could not be
     *                     deleted for some reason, for example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfWindows(FileUtils::deleteDirectoryInternal, directory);
    }

    /**
     * Deletes the given directory recursively, not reporting any I/O exceptions
     * that occur.
     *
     * <p>This method is identical to {@link FileUtils#deleteDirectory(File)}, except that it
     * swallows all exceptions and may leave the job quietly incomplete.
     *
     * @param directory The directory to delete.
     */
    public static void deleteDirectoryQuietly(File directory) {
        if (directory == null) {
            return;
        }

        // delete and do not report if it fails
        try {
            deleteDirectory(directory);
        } catch (Exception ignored) {}
    }

    /**
     * Removes all files contained within a directory, without removing the directory itself.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to remove all files from.
     *
     * @throws FileNotFoundException Thrown if the directory itself does not exist.
     * @throws IOException Thrown if the file indicates a proper file and not a directory, or if
     *                     the directory could not be cleaned for some reason, for example
     *                     due to missing access/write permissions.
     */
    public static void cleanDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfWindows(FileUtils::cleanDirectoryInternal, directory);
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

    private static void deleteDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            // directory exists and is a directory

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
        else if (directory.exists()) {
            // exists but is file, not directory
            // either an error from the caller, or concurrently a file got created
            throw new IOException(directory + " is not a directory");
        }
        // else: does not exist, which is okay (as if deleted)
    }

    private static void cleanDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                } else {
                    throw new FileNotFoundException(directory.toString());
                }
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectory(file);
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

    private static void guardIfWindows(ThrowingConsumer<File, IOException> toRun, File file) throws IOException {
        if (!OperatingSystem.isWindows()) {
            toRun.accept(file);
        }
        else {
            // for windows, we synchronize on a global lock, to prevent concurrent delete issues
            // >
            // in the future, we may want to find either a good way of working around file visibility
            // in Windows under concurrent operations (the behavior seems completely unpredictable)
            // or  make this locking more fine grained, for example  on directory path prefixes
            synchronized (WINDOWS_DELETE_LOCK) {
                for (int attempt = 1; attempt <= 10; attempt++) {
                    try {
                        toRun.accept(file);
                        break;
                    }
                    catch (AccessDeniedException e) {
                        // ah, windows...
                    }

                    // briefly wait and fall through the loop
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // restore the interruption flag and error out of the method
                        Thread.currentThread().interrupt();
                        throw new IOException("operation interrupted");
                    }
                }
            }
        }
    }


    // ------------------------------------------------------------------------

    /**
     * Private default constructor to avoid instantiation.
     */
    private FileUtils() {}

}
