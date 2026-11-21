package media.consumer;

import media.common.UploadConfig;
import media.grpc.*;
import io.grpc.stub.StreamObserver;
import java.io.*;
import java.nio.file.*;

/** gRPC controller: validates metadata and streams each file to a temporary path. */
public final class UploadService extends UploadServiceGrpc.UploadServiceImplBase {
    private final MediaService media;
    private final java.util.function.Consumer<String> log;

    public UploadService(MediaService media, java.util.function.Consumer<String> log) {
        this.media = media; this.log = log;
    }

    @Override public StreamObserver<UploadRequest> upload(StreamObserver<UploadReply> response) {
        return new StreamObserver<>() {
            private boolean done, receivedInfo;
            private Path temp;
            private OutputStream output;
            private String filename;
            private long expected, bytes;
            private String sha256;

            private void finish(boolean success, String message) {
                if (done) return;
                done = true;
                try { if (output != null) output.close(); } catch (IOException ignored) {}
                if (!success) {
                    if (temp != null) try { Files.deleteIfExists(temp); } catch (IOException ignored) {}
                }
                log.accept(message);
                response.onNext(UploadReply.newBuilder().setAccepted(success).setMessage(message).build());
                response.onCompleted();
            }

            @Override public void onNext(UploadRequest request) {
                if (done) return;
                try {
                    if (!receivedInfo) {
                        if (!request.hasInfo()) { finish(false, "Missing initial file metadata"); return; }
                        var info = request.getInfo();
                        filename = info.getFilename(); expected = info.getSize();
                        sha256 = info.getSha256();
                        if (filename.isBlank() || expected <= 0 || expected > UploadConfig.MAX_FILE_BYTES ||
                                !filename.toLowerCase().matches(".*\\.(mp4|m4v|mov|mkv|webm)")) {
                            finish(false, "Invalid filename, extension, or file size"); return;
                        }
                        temp = Files.createTempFile("media-incoming-", ".part");
                        output = Files.newOutputStream(temp);
                        receivedInfo = true;
                    } else if (request.hasChunk()) {
                        var chunk = request.getChunk();
                        if (bytes + chunk.size() > expected) { finish(false, "Upload exceeds declared size"); return; }
                        chunk.writeTo(output); bytes += chunk.size();
                    } else finish(false, "Metadata must appear only once");
                } catch (IOException e) { finish(false, "Upload I/O failure: " + e.getMessage()); }
            }

            @Override public void onError(Throwable t) {
                if (done) return;
                done = true;
                try { if (output != null) output.close(); } catch (IOException ignored) {}
                if (temp != null) try { Files.deleteIfExists(temp); } catch (IOException ignored) {}
                log.accept("Client disconnected: " + t.getMessage());
            }

            @Override public void onCompleted() {
                if (done) return;
                if (!receivedInfo || bytes != expected) { finish(false, "Upload incomplete: " + bytes + "/" + expected); return; }
                try { output.close(); output = null; }
                catch (IOException e) { finish(false, "Close failed: " + e.getMessage()); return; }
                String uploadedHash = sha256;
                try { uploadedHash = MediaService.sha256(temp); }
                catch (IOException e) { finish(false, "Hash calculation failed: " + e.getMessage()); return; }
                MediaService.SubmitResult result = media.submit(temp, filename, bytes, uploadedHash);
                if (result == MediaService.SubmitResult.DUPLICATE)
                    finish(false, "DUPLICATE: file already exists (" + filename + ")");
                else if (result == MediaService.SubmitResult.QUEUE_FULL)
                    finish(false, "DROPPED: queue full");
                else { temp = null; finish(true, "Accepted " + filename + " (" + bytes + " bytes)"); }
            }
        };
    }
}
