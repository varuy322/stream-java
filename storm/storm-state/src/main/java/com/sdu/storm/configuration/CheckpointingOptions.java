package com.sdu.storm.configuration;

public class CheckpointingOptions {

    /** Option whether the state backend should create incremental checkpoints,
     * if possible. For an incremental checkpoint, only a diff from the previous
     * checkpoint is stored, rather than the complete checkpoint state.
     *
     * <p>Some state backends may not support incremental checkpoints and ignore this option. </p>
     *
     * */
    public static final ConfigOption<Boolean> INCREMENTAL_CHECKPOINTS = ConfigOptions
            .<Boolean>key("state.backend.incremental")
            .defaultValue(false)
            .description("Option whether the state backend should create incremental checkpoints, if possible. For" +
                    " an incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the" +
                    " complete checkpoint state. Some state backends may not support incremental checkpoints and ignore" +
                    " this option.")
            .build();

}
