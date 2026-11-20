package media.producer;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;
import java.nio.file.Path;
import java.util.ArrayList;

/** Producer GUI (view/controller); p is the number of distinct source directories. */
public final class ProducerView extends Application {
    @Override public void start(Stage stage) {
        TextField host = new TextField("127.0.0.1"), port = new TextField("50051"), p = new TextField("2");
        host.setPrefColumnCount(16); port.setPrefColumnCount(6); p.setPrefColumnCount(5);
        ListView<Path> directories = new ListView<>();
        TextArea log = new TextArea(); log.setEditable(false);
        Button add = new Button("Add Video Folder"), remove = new Button("Remove Selected"), send = new Button("Start Uploads");
        add.setOnAction(e -> {
            DirectoryChooser chooser = new DirectoryChooser(); chooser.setTitle("Select a unique producer folder");
            var dir = chooser.showDialog(stage);
            if (dir != null) {
                Path selected = dir.toPath().toAbsolutePath();
                if (!directories.getItems().contains(selected)) directories.getItems().add(selected);
            }
        });
        remove.setOnAction(e -> directories.getItems().remove(directories.getSelectionModel().getSelectedItem()));
        send.setOnAction(e -> {
            try {
                int count = media.common.UploadConfig.positive(p.getText(), "p");
                int serverPort = Integer.parseInt(port.getText().trim());
                if (serverPort < 1 || serverPort > 65535 || host.getText().isBlank()) throw new IllegalArgumentException("Invalid host/port");
                if (directories.getItems().size() != count) throw new IllegalArgumentException("Choose exactly p=" + count + " distinct folders");
                new ProducerService(message -> Platform.runLater(() -> log.appendText(message + "\n")))
                    .uploadFolders(host.getText().trim(), serverPort, new ArrayList<>(directories.getItems()));
            } catch (Exception ex) { new Alert(Alert.AlertType.ERROR, ex.getMessage()).showAndWait(); }
        });
        VBox root = new VBox(10,
            new HBox(8, new Label("Consumer IP"), host, new Label("Port"), port, new Label("p"), p),
            new Label("Each producer thread reads one unique folder:"),
            directories, new HBox(8, add, remove, send), new Label("Upload results"), log);
        root.setPadding(new Insets(16)); VBox.setVgrow(directories, Priority.ALWAYS); VBox.setVgrow(log, Priority.ALWAYS);
        stage.setTitle("Media Upload Service | Producer");
        stage.setScene(new Scene(root, 790, 590)); stage.show();
    }
    public static void main(String[] args) { launch(args); }
}
