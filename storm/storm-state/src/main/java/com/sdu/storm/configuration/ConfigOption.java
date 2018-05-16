package com.sdu.storm.configuration;

import java.util.Arrays;
import java.util.Collections;

import static com.sdu.storm.utils.Preconditions.checkNotNull;

public class ConfigOption<T> {

    private static final String[] EMPTY = new String[0];

    /** The current key for that config option. */
    private final String key;

    /** The list of deprecated keys, in the order to be checked. */
    private String[] deprecatedKeys;

    /** The default value for this option. */
    private T defaultValue;

    /** The description for this option. */
    private String description;

    // ------------------------------------------------------------------------

    /**
     * Creates a new config option with no deprecated keys.
     *
     * @param key             The current key for that config option
     */
    ConfigOption(String key) {
        this.key = checkNotNull(key);
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the configuration key.
     * @return The configuration key
     */
    public String key() {
        return key;
    }

    /**
     * Checks if this option has a default value.
     * @return True if it has a default value, false if not.
     */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Returns the default value, or null, if there is no default value.
     * @return The default value, or null.
     */
    public T defaultValue() {
        return defaultValue;
    }

    /**
     * Checks whether this option has deprecated keys.
     * @return True if the option has deprecated keys, false if not.
     */
    public boolean hasDeprecatedKeys() {
        return deprecatedKeys != EMPTY;
    }

    /**
     * Gets the deprecated keys, in the order to be checked.
     * @return The option's deprecated keys.
     */
    public Iterable<String> deprecatedKeys() {
        return deprecatedKeys == EMPTY ? Collections.<String>emptyList() : Arrays.asList(deprecatedKeys);
    }

    void setDeprecatedKeys(String ... deprecatedKeys) {
        this.deprecatedKeys = deprecatedKeys;
    }

    /**
     * Returns the description of this option.
     * @return The option's description.
     */
    public String description() {
        return description;
    }

    void setDescription(String description) {
        this.description = description;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && o.getClass() == ConfigOption.class) {
            ConfigOption<?> that = (ConfigOption<?>) o;
            return this.key.equals(that.key) &&
                    Arrays.equals(this.deprecatedKeys, that.deprecatedKeys) &&
                    (this.defaultValue == null ? that.defaultValue == null :
                            (that.defaultValue != null && this.defaultValue.equals(that.defaultValue)));
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() +
                17 * Arrays.hashCode(deprecatedKeys) +
                (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format("Key: '%s' , default: %s (deprecated keys: %s)",
                key, defaultValue, Arrays.toString(deprecatedKeys));
    }
}
