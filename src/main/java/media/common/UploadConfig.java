package media.common;

public final class UploadConfig {
    public static final int CHUNK_SIZE = 64 * 1024;
    public static final long MAX_FILE_BYTES = 500L * 1024 * 1024;
    private UploadConfig() {}
    public static int positive(String value, String name) {
        int parsed = Integer.parseInt(value.trim());
        if (parsed < 1) throw new IllegalArgumentException(name + " must be at least 1");
        return parsed;
    }
}
