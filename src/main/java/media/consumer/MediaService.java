package media.consumer;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Model: bounded pending uploads; fixed worker pool persists files. */
public final class MediaService implements AutoCloseable {
    private final BlockingQueue<Pending> queue;
    private final Semaphore capacity;
    private final ExecutorService workers;
    private final Path destination;
    private final Consumer<MediaRecord> onSaved;
    private final Consumer<String> onEvent;
    private final AtomicInteger active = new AtomicInteger();
    private volatile boolean running = true;

    record Pending(Path temporary, String filename, long size) {}

    public MediaService(int consumers, int maxQueue, Path destination,
                        Consumer<MediaRecord> onSaved, Consumer<String> onEvent) throws IOException {
        if (consumers < 1 || maxQueue < 1) throw new IllegalArgumentException("c and q must be positive");
        this.queue = new ArrayBlockingQueue<>(maxQueue);
        this.capacity = new Semaphore(maxQueue);
        this.destination = Files.createDirectories(destination.toAbsolutePath());
        this.onSaved = onSaved;
        this.onEvent = onEvent;
        workers = Executors.newFixedThreadPool(consumers, r -> {
            Thread thread = new Thread(r, "consumer-worker"); thread.setDaemon(true); return thread;
        });
        for (int i = 0; i < consumers; i++) workers.execute(this::consume);
    }

    /** Reserve pending capacity before accepting a byte of an upload. */
    public boolean reserve() { return running && capacity.tryAcquire(); }
    public void release() { capacity.release(); }
    public boolean submit(Path temp, String filename, long size) {
        if (!running || !queue.offer(new Pending(temp, filename, size))) return false;
        onEvent.accept("Queued " + filename + " | waiting=" + queue.size() + " active=" + active.get());
        return true;
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
