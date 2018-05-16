package com.sdu.storm.configuration;

import static com.sdu.storm.utils.Preconditions.checkNotNull;

public class ConfigOptions {

    /**
     * Starts building a new {@link ConfigOption}.
     *
     * @param key The key for the config option.
     * @return The builder for the config option with the given key.
     */
    public static <T> OptionBuilder<T> key(String key) {
        checkNotNull(key);
        return new OptionBuilder<>(key);
    }

    public static final class OptionBuilder<T> {

        private ConfigOption<T> option;

        /**
         * Creates a new OptionBuilder.
         * @param key The key for the config option
         */
        OptionBuilder(String key) {
            this.option = new ConfigOption<>(key);
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * <p>This method does not accept "null". For options with no default value, choose
         * one of the {@code noDefaultValue} methods.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public OptionBuilder<T> defaultValue(T value) {
            checkNotNull(value);
            option.setDefaultValue(value);
            return this;
        }


        public OptionBuilder<T> description(String desc) {
            checkNotNull(desc);
            option.setDescription(desc);
            return this;
        }

        public OptionBuilder<T> deprecatedKeys(String ... deprecatedKeys) {
            checkNotNull(deprecatedKeys);
            option.setDeprecatedKeys(deprecatedKeys);
            return this;
        }

        public ConfigOption<T> build() {
            return this.option;
        }
    }

}
