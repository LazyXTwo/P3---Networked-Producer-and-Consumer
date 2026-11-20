package media.consumer;

import java.nio.file.Path;
public record MediaRecord(String filename, Path path, long bytes) {
    @Override public String toString() { return filename + " (" + (bytes / 1024) + " KB)"; }
}
