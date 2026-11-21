package media.producer;

import com.google.protobuf.ByteString;
import media.common.UploadConfig;
import media.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Producer model: one task/thread per source directory, each using its own gRPC stream per video. */
public final class ProducerService {
    private final Consumer<String> log;
    public ProducerService(Consumer<String> log) { this.log = log; }

    public void uploadFolders(String host, int port, List<Path> folders) {
        if (folders.isEmpty()) throw new IllegalArgumentException("Select at least one folder");
        Thread coordinator = new Thread(() -> {
            ManagedChannel channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
            ExecutorService producers = Executors.newFixedThreadPool(folders.size());
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < folders.size(); i++) {
                    final int id = i + 1;
                    final Path folder = folders.get(i);
                    futures.add(producers.submit(() -> processFolder(channel, id, folder)));
                }
                for (Future<?> future : futures) {
                    try { future.get(); } catch (Exception e) { log.accept("Producer failed: " + e.getMessage()); }
                }
                log.accept("All producers finished");
            } finally {
                producers.shutdown(); channel.shutdown();
            }
        }, "producer-coordinator");
        coordinator.setDaemon(true); coordinator.start();
    }

    private void processFolder(ManagedChannel channel, int id, Path folder) {
        if (!Files.isDirectory(folder)) { log.accept("P" + id + ": folder does not exist: " + folder); return; }
        try (var files = Files.list(folder)) {
            List<Path> videos = files.filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().toLowerCase().matches(".*\\.(mp4|m4v|mov|mkv|webm)"))
                .sorted().toList();
            log.accept("P" + id + " found " + videos.size() + " videos in " + folder);
            for (Path video : videos) upload(channel, id, video);
        } catch (Exception e) { log.accept("P" + id + " folder error: " + e.getMessage()); }
    }

    private void upload(ManagedChannel channel, int id, Path video) {
        try {
            String sha256 = sha256(video);
            long size = Files.size(video);
            if (size <= 0 || size > UploadConfig.MAX_FILE_BYTES) {
                log.accept("P" + id + " skipped invalid size: " + video.getFileName()); return;
            }
            CountDownLatch finished = new CountDownLatch(1);
            final String[] responseMessage = {"No server reply"};
            final boolean[] serverRejected = {false};
            StreamObserver<UploadReply> replyObserver = new StreamObserver<>() {
                @Override public void onNext(UploadReply reply) {
                    responseMessage[0] = reply.getMessage();
                    if (!reply.getAccepted()) serverRejected[0] = true;
                }
                @Override public void onError(Throwable error) { responseMessage[0] = "RPC failure: " + error.getMessage(); finished.countDown(); }
                @Override public void onCompleted() { finished.countDown(); }
            };
            // The asynchronous stub permits multiple concurrent producer streams.
            StreamObserver<UploadRequest> stream = UploadServiceGrpc.newStub(channel).upload(replyObserver);
            stream.onNext(UploadRequest.newBuilder().setInfo(FileInfo.newBuilder()
                .setFilename(video.getFileName().toString()).setSize(size).setSha256(sha256)).build());
            try (InputStream input = Files.newInputStream(video)) {
                byte[] buffer = new byte[UploadConfig.CHUNK_SIZE];
                int n;
                while ((n = input.read(buffer)) != -1) {
                    if (finished.getCount() == 0 || serverRejected[0]) break;
                    stream.onNext(UploadRequest.newBuilder()
                        .setChunk(ByteString.copyFrom(buffer, 0, n)).build());
                }
            }
            if (finished.getCount() != 0 && !serverRejected[0]) stream.onCompleted();
            if (!finished.await(120, TimeUnit.SECONDS)) {
                stream.onError(new TimeoutException("Upload timed out"));
                log.accept("P" + id + " timed out: " + video.getFileName());
            } else log.accept("P" + id + " " + video.getFileName() + " -> " + responseMessage[0]);
        } catch (Exception e) { log.accept("P" + id + " failed " + video.getFileName() + ": " + e.getMessage()); }
    }

    private String sha256(Path video) throws IOException {
        try (var input = Files.newInputStream(video)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[UploadConfig.CHUNK_SIZE];
            int read;
            while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

}
