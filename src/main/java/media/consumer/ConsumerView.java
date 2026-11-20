package media.consumer;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.stage.Stage;
import javafx.util.Duration;
import java.nio.file.Path;

/** Consumer GUI (view/controller); launch on the consumer VM. */
public final class ConsumerView extends Application {
    private final ObservableList<MediaRecord> videos = FXCollections.observableArrayList();
    private final ListView<MediaRecord> list = new ListView<>(videos);
    private final TextArea events = new TextArea();
    private final MediaView playerView = new MediaView();
    private Label status;
    private MediaPlayer player;
    private MediaService model;
    private Server server;

    @Override public void start(Stage stage) {
        TextField port = new TextField("50051"), consumers = new TextField("2"), queue = new TextField("5"),
            folder = new TextField("uploads");
        port.setPrefColumnCount(6); consumers.setPrefColumnCount(4); queue.setPrefColumnCount(4);
        folder.setPrefColumnCount(14);
        Button start = new Button("Start Consumer");
        status = new Label("Not running");
        HBox settings = new HBox(8, new Label("Port"), port, new Label("c"), consumers,
            new Label("q"), queue, new Label("Save to"), folder, start);
        settings.setPadding(new Insets(10));
        list.setPrefWidth(310);
        list.setPlaceholder(new Label("No uploaded videos yet"));
        playerView.setFitWidth(640); playerView.setFitHeight(360); playerView.setPreserveRatio(true);
        StackPane screen = new StackPane(playerView);
        screen.setStyle("-fx-background-color: #151515;");
        screen.setMinHeight(365);
        Label help = new Label("Hover an item for a 10-second preview. Click to play the full video.");
        VBox right = new VBox(8, help, screen, new Label("Events"), events);
        VBox.setVgrow(events, Priority.ALWAYS);
        HBox content = new HBox(12, list, right);
        HBox.setHgrow(right, Priority.ALWAYS);
        events.setEditable(false); events.setPrefRowCount(7);
        BorderPane root = new BorderPane(content);
        root.setTop(new VBox(settings, status)); root.setPadding(new Insets(10));
        start.setOnAction(e -> {
            try {
                int c = media.common.UploadConfig.positive(consumers.getText(), "c");
                int q = media.common.UploadConfig.positive(queue.getText(), "q");
                int p = Integer.parseInt(port.getText().trim());
                if (p < 1 || p > 65535) throw new IllegalArgumentException("Invalid port");
                model = new MediaService(c, q, Path.of(folder.getText()),
                    record -> Platform.runLater(() -> videos.add(record)), this::log);
                videos.addAll(model.existingFiles());
                server = NettyServerBuilder.forPort(p).addService(new UploadService(model, this::log)).build().start();
                start.setDisable(true); port.setDisable(true); consumers.setDisable(true);
                queue.setDisable(true); folder.setDisable(true);
                status.setText("Listening on port " + p + " | workers=" + c + " | capacity=" + q);
                log("Consumer ready on port " + p);
            } catch (Exception ex) {
                if (model != null) { model.close(); model = null; }
                showError(ex.getMessage());
            }
        });
        list.setCellFactory(view -> {
            ListCell<MediaRecord> cell = new ListCell<>() {
                @Override protected void updateItem(MediaRecord item, boolean empty) {
                    super.updateItem(item, empty); setText(empty || item == null ? null : item.toString());
                }
            };
            cell.setOnMouseEntered(e -> { if (!cell.isEmpty()) play(cell.getItem(), true); });
            cell.setOnMouseExited(e -> { if (preview) stopPlayer(); });
            cell.setOnMouseClicked(e -> { if (!cell.isEmpty()) play(cell.getItem(), false); });
            return cell;
        });
        stage.setTitle("Media Upload Service | Consumer");
        stage.setScene(new Scene(root, 1060, 670)); stage.show();
    }

    private boolean preview;
    private void play(MediaRecord video, boolean isPreview) {
        try {
            stopPlayer(); preview = isPreview;
            player = new MediaPlayer(new Media(video.path().toUri().toString()));
            playerView.setMediaPlayer(player);
            player.setOnError(() -> log("Playback error: " + player.getError()));
            if (isPreview) player.setStopTime(Duration.seconds(10));
            player.play();
            status.setText((isPreview ? "Previewing first 10s: " : "Playing: ") + video.filename());
        } catch (Exception ex) { log("Playback unavailable: " + ex.getMessage()); }
    }
    private void stopPlayer() {
        if (player != null) { player.stop(); player.dispose(); player = null; }
        preview = false;
    }
    private void log(String message) {
        Platform.runLater(() -> events.appendText(message + System.lineSeparator()));
    }
    private void showError(String message) { new Alert(Alert.AlertType.ERROR, message).showAndWait(); }
    @Override public void stop() {
        stopPlayer(); if (server != null) server.shutdownNow(); if (model != null) model.close();
    }
    public static void main(String[] args) { launch(args); }
}
