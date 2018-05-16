package com.sdu.storm.fs;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;
import com.sdu.storm.utils.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class Path implements IOReadableWritable, Serializable {

    /**
     * The directory separator, a slash.
     */
    public static final String SEPARATOR = "/";

    /** A pre-compiled regex/state-machine to match the windows drive pattern. */
    private static final Pattern WINDOWS_ROOT_DIR_REGEX = Pattern.compile("/\\p{Alpha}+:/");

    // 基本形式: [scheme:] scheme-specific-part        [#fragment]
    // 进步细分: [scheme:] [//authority][path][?query] [#fragment]
    // 中级细分: [scheme:] [//host:port][path][?query] [#fragment]
    private URI uri;

    public Path(String pathString) {
        pathString = checkAndTrimPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }

    public Path(URI uri) {
        this.uri = uri;
    }

    public URI toUri() {
        return uri;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        if (this.uri == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            StringUtils.writeNullableString(uri.getScheme(), out);
            StringUtils.writeNullableString(uri.getUserInfo(), out);
            StringUtils.writeNullableString(uri.getHost(), out);
            out.writeInt(uri.getPort());
            StringUtils.writeNullableString(uri.getPath(), out);
            StringUtils.writeNullableString(uri.getQuery(), out);
            StringUtils.writeNullableString(uri.getFragment(), out);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        boolean isNotNull = in.readBoolean();
        if (isNotNull) {
            final String scheme = StringUtils.readNullableString(in);
            final String userInfo = StringUtils.readNullableString(in);
            final String host = StringUtils.readNullableString(in);
            final int port = in.readInt();
            final String path = StringUtils.readNullableString(in);
            final String query = StringUtils.readNullableString(in);
            final String fragment = StringUtils.readNullableString(in);

            try {
                uri = new URI(scheme, userInfo, host, port, path, query, fragment);
            } catch (URISyntaxException e) {
                throw new IOException("Error reconstructing URI", e);
            }
        }
    }

    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private String normalizePath(String path) {

        // remove leading and tailing whitespaces
        path = path.trim();

        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if (path.endsWith(SEPARATOR) &&
                !path.equals(SEPARATOR) &&              // UNIX root path
                !WINDOWS_ROOT_DIR_REGEX.matcher(path).matches()) {  // Windows root path)

            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }

    private static String checkAndTrimPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        path = path.trim();
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    private static boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a' && path
                .charAt(start) <= 'z'));
    }

}
