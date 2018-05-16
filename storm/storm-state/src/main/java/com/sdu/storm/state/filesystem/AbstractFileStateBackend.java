package com.sdu.storm.state.filesystem;

import com.sdu.storm.fs.Path;
import com.sdu.storm.state.AbstractStateBackend;

import javax.annotation.Nullable;
import java.net.URI;

public abstract class AbstractFileStateBackend extends AbstractStateBackend {

    /** The path where checkpoints will be stored, or null, if none has been configured. */
    @Nullable
    private final Path baseCheckpointPath;

    /** The path where savepoints will be stored, or null, if none has been configured. */
    @Nullable
    private final Path baseSavepointPath;


    protected AbstractFileStateBackend(
            @Nullable URI baseCheckpointPath,
            @Nullable URI baseSavepointPath) {
        this(baseCheckpointPath == null ? null : new Path(baseCheckpointPath),
                baseSavepointPath == null ? null : new Path(baseSavepointPath));
    }


    protected AbstractFileStateBackend(@Nullable Path baseCheckpointPath,
                                       @Nullable Path baseSavepointPath) {
        this.baseCheckpointPath = baseCheckpointPath == null ? null : validatePath(baseCheckpointPath);
        this.baseSavepointPath = baseSavepointPath == null ? null : validatePath(baseSavepointPath);
    }


    private static Path validatePath(Path path) {
        final URI uri = path.toUri();
        final String scheme = uri.getScheme();
        final String pathPart = uri.getPath();

        // some validity checks
        if (scheme == null) {
            throw new IllegalArgumentException("The scheme (hdfs://, file://, etc) is null. " +
                    "Please specify the file system scheme explicitly in the URI.");
        }
        if (pathPart == null) {
            throw new IllegalArgumentException("The path to store the checkpoint data in is null. " +
                    "Please specify a directory path for the checkpoint data.");
        }
        if (pathPart.length() == 0 || pathPart.equals("/")) {
            throw new IllegalArgumentException("Cannot use the root directory for checkpoints.");
        }

        return path;
    }

    @Nullable
    public Path getCheckpointPath() {
        return baseCheckpointPath;
    }

    @Nullable
    public Path getSavepointPath() {
        return baseSavepointPath;
    }
}
