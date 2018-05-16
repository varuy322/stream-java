package com.sdu.storm.state.filesystem;

import com.sdu.storm.fs.Path;
import com.sdu.storm.state.AbstractKeyedStateBackend;
import com.sdu.storm.state.KeyGroupRange;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.TernaryBoolean;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static com.sdu.storm.utils.Preconditions.checkArgument;
import static com.sdu.storm.utils.Preconditions.checkNotNull;

public class FsStateBackend extends AbstractFileStateBackend {

    /** Maximum size of state that is stored with the metadata, rather than in files (1 MiByte). */
    public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;

    // ------------------------------------------------------------------------

    /** State below this size will be stored as part of the metadata, rather than in files.
     * A value of '-1' means not yet configured, in which case the default will be used. */
    private final int fileStateThreshold;

    /** Switch to chose between synchronous and asynchronous snapshots.
     * A value of 'undefined' means not yet configured, in which case the default will be used. */
    private final TernaryBoolean asynchronousSnapshots;

    // ------------------------------------------------------------------------


    public FsStateBackend(String checkpointDataUri) {
        this(new Path(checkpointDataUri).toUri());
    }

    public FsStateBackend(URI checkpointDataUri) {
        this(checkpointDataUri, null, -1, TernaryBoolean.UNDEFINED);
    }

    /**
     * Creates a new state backend that stores its checkpoint data in the file system and location
     * defined by the given URI.
     *
     * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
     * must be accessible via {@link FileSystem#get(URI)}.
     *
     * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
     * (host and port), or that the Hadoop configuration that describes that information must be in the
     * classpath.
     *
     * @param checkpointDirectory        The path to write checkpoint metadata to.
     * @param defaultSavepointDirectory  The path to write savepoints to. If null, the value from
     *                                   the runtime configuration will be used, or savepoint
     *                                   target locations need to be passed when triggering a savepoint.
     * @param fileStateSizeThreshold     State below this size will be stored as part of the metadata,
     *                                   rather than in files. If -1, the value configured in the
     *                                   runtime configuration will be used, or the default value (1KB)
     *                                   if nothing is configured.
     * @param asynchronousSnapshots      Flag to switch between synchronous and asynchronous
     *                                   snapshot mode. If UNDEFINED, the value configured in the
     *                                   runtime configuration will be used.
     */
    public FsStateBackend(
            URI checkpointDirectory,
            @Nullable URI defaultSavepointDirectory,
            int fileStateSizeThreshold,
            TernaryBoolean asynchronousSnapshots) {

        super(checkNotNull(checkpointDirectory, "checkpoint directory is null"), defaultSavepointDirectory);

        checkNotNull(asynchronousSnapshots, "asynchronousSnapshots");
        checkArgument(fileStateSizeThreshold >= -1 && fileStateSizeThreshold <= MAX_FILE_STATE_THRESHOLD,
                "The threshold for file state size must be in [-1, %s], where '-1' means to use " +
                        "the value from the deployment's configuration.", MAX_FILE_STATE_THRESHOLD);

        this.fileStateThreshold = fileStateSizeThreshold;
        this.asynchronousSnapshots = asynchronousSnapshots;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Map stormConf,
            String component,
            int taskId,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) throws IOException {
        return null;
    }

}
