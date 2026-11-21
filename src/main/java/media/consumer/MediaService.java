package media.consumer;

import media.common.UploadConfig;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Model: bounded pending uploads; fixed worker pool persists files. */
public final class MediaService implements AutoCloseable {
    private final BlockingQueue<Pending> queue;
    private final Semaphore capacity;
    private final ExecutorService workers;
    private final Path destination;
    private final Consumer<MediaRecord> onSaved;
    private final Consumer<String> onEvent;
    private final AtomicInteger active = new AtomicInteger();
    private final Set<String> knownHashes = ConcurrentHashMap.newKeySet();
    private volatile boolean running = true;

    public enum SubmitResult { ACCEPTED, DUPLICATE, QUEUE_FULL }
    record Pending(Path temporary, String filename, long size, String sha256) {}
    private record Compressed(Path path, String filename, long size) {}

    public MediaService(int consumers, int maxQueue, Path destination,
                        Consumer<MediaRecord> onSaved, Consumer<String> onEvent) throws IOException {
        if (consumers < 1 || maxQueue < 1) throw new IllegalArgumentException("c and q must be positive");
        this.queue = new ArrayBlockingQueue<>(maxQueue);
        this.capacity = new Semaphore(maxQueue);
        this.destination = Files.createDirectories(destination.toAbsolutePath());
        this.onSaved = onSaved;
        this.onEvent = onEvent;
        try (var paths = Files.list(destination)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String hash = sha256(path);
                    if (!hash.isBlank()) knownHashes.add(hash);
                } catch (IOException ignored) {}
            });
        }
        workers = Executors.newFixedThreadPool(consumers, r -> {
            Thread thread = new Thread(r, "consumer-worker"); thread.setDaemon(true); return thread;
        });
        for (int i = 0; i < consumers; i++) workers.execute(this::consume);
    }

    /** Reserve pending capacity before accepting a byte of an upload. */
    public boolean reserve() { return running && capacity.tryAcquire(); }
    public void release() { capacity.release(); }
    public boolean isDuplicate(String sha256) {
        return sha256 != null && !sha256.isBlank() && knownHashes.contains(sha256);
    }

    public SubmitResult submit(Path temp, String filename, long size, String sha256) {
        if (!running) return SubmitResult.QUEUE_FULL;

        Path uploadTemp = temp;
        String uploadFilename = filename;
        long uploadSize = size;
        Compressed compressed = compress(temp, filename);
        if (compressed != null) {
            if (compressed.path() != temp) {
                try { Files.deleteIfExists(temp); } catch (IOException ignored) {}
            }
            uploadTemp = compressed.path();
            uploadFilename = compressed.filename();
            uploadSize = compressed.size();
        }

        try {
            sha256 = sha256(uploadTemp);
        } catch (IOException e) {
            onEvent.accept("Hash failed: " + e.getMessage());
            try { Files.deleteIfExists(uploadTemp); } catch (IOException ignored) {}
            return SubmitResult.QUEUE_FULL;
        }

        synchronized (knownHashes) {
            if (sha256 != null && !sha256.isBlank() && knownHashes.contains(sha256)) {
                try { Files.deleteIfExists(uploadTemp); } catch (IOException ignored) {}
                return SubmitResult.DUPLICATE;
            }
            if (!queue.offer(new Pending(uploadTemp, uploadFilename, uploadSize, sha256))) {
                try { Files.deleteIfExists(uploadTemp); } catch (IOException ignored) {}
                return SubmitResult.QUEUE_FULL;
            }
            if (sha256 != null && !sha256.isBlank()) knownHashes.add(sha256);
        }
        onEvent.accept("Queued " + uploadFilename + " | waiting=" + queue.size() + " active=" + active.get());
        return SubmitResult.ACCEPTED;
    }

    private Compressed compress(Path input, String originalFilename) {
        try {
            int dot = originalFilename.lastIndexOf('.');
            String stem = dot > 0 ? originalFilename.substring(0, dot) : originalFilename;
            String safeStem = stem.replaceAll("[^a-zA-Z0-9._-]", "_");
            if (safeStem.isBlank()) safeStem = "video";
            Path output = Files.createTempFile("media-compressed-" + safeStem + "-", ".mp4");
            Files.delete(output);

            Process process = new ProcessBuilder(
                "ffmpeg", "-y", "-i", input.toString(),
                "-vcodec", "libx264", "-crf", "28",
                "-preset", "veryfast", "-acodec", "aac", "-b:a", "128k",
                output.toString())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();

            if (!process.waitFor(10, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                Files.deleteIfExists(output);
                onEvent.accept("Compression timed out: " + originalFilename);
                return null;
            }
            if (process.exitValue() != 0 || !Files.isRegularFile(output) || Files.size(output) == 0) {
                Files.deleteIfExists(output);
                onEvent.accept("Compression failed: " + originalFilename);
                return null;
            }

            onEvent.accept("Compressed " + originalFilename + " to " + Files.size(output) + " bytes");
            return new Compressed(output, safeStem + ".mp4", Files.size(output));
        } catch (IOException e) {
            onEvent.accept("Compression unavailable (FFmpeg required): " + e.getMessage());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onEvent.accept("Compression interrupted");
            return null;
        }
    }

    public static String sha256(Path path) throws IOException {
        try (var input = Files.newInputStream(path)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[UploadConfig.CHUNK_SIZE];
            int read;
            while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private void consume() {
        while (running || !queue.isEmpty()) {
            Pending pending;
            try { pending = queue.poll(250, TimeUnit.MILLISECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            if (pending == null) continue;
            active.incrementAndGet();
            try {
                String original = pending.filename();
                String base = original.replaceAll("[^a-zA-Z0-9._-]", "_");
                if (base.isBlank() || base.equals(".") || base.equals("..")) base = "video.mp4";
                String suffix = base.contains(".") ? base.substring(base.lastIndexOf('.')) : "";
                String stem = suffix.isEmpty() ? base : base.substring(0, base.length() - suffix.length());
                Path target = destination.resolve(base);
                int number = 1;
                while (Files.exists(target)) target = destination.resolve(stem + "_" + number++ + suffix);
                Files.move(pending.temporary(), target);
                onSaved.accept(new MediaRecord(target.getFileName().toString(), target, pending.size()));
                onEvent.accept("Saved " + target.getFileName());
            } catch (IOException e) {
                onEvent.accept("Save failed: " + e.getMessage());
                try { Files.deleteIfExists(pending.temporary()); } catch (IOException ignored) {}
            } finally { active.decrementAndGet(); capacity.release(); }
        }
    }

    public List<MediaRecord> existingFiles() throws IOException {
        try (var paths = Files.list(destination)) {
            return paths.filter(Files::isRegularFile)
                .filter(p -> p.toString().toLowerCase().matches(".*\\.(mp4|m4v|mov|mkv|webm)"))
                .map(p -> { try { return new MediaRecord(p.getFileName().toString(), p, Files.size(p)); }
                    catch (IOException e) { return null; } }).filter(java.util.Objects::nonNull).toList();
        }
    }
    @Override public void close() { running = false; workers.shutdown(); }
}
