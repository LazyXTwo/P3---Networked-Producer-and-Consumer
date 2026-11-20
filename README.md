# P3: Networked Producer and Consumer

Java-based media upload simulation using **gRPC**, **multithreading**, **bounded queueing**, and **JavaFX**. The Producer and Consumer are intended to run on **different virtual machines**. Both applications use the same `media.proto` contract.

## 1. Required software and setup

- **JDK 17 or later**, with `java` and `javac` available in the terminal.
- **Apache Maven 3.9 or later**, available through the `mvn` command.
- An internet connection for Maven's initial dependency download.
- A graphical desktop session for each JavaFX GUI. A headless server cannot display these windows.
- Network access between the VMs, with **TCP port 50051** open on the Consumer VM.
- Sample videos, preferably **H.264/AAC `.mp4`** files for JavaFX playback.

Check the installation:

```bash
java -version
javac -version
mvn -version
```

The Maven project automatically downloads JavaFX, gRPC, Protobuf, and other dependencies listed in `pom.xml`. **Node.js and Python are not needed.**

## 2. Repository structure

```text
networked-media/
├── pom.xml
├── READ.md
└── src/main/
    ├── proto/
    │   └── media.proto
    └── java/media/
        ├── common/
        │   └── UploadConfig.java
        ├── producer/
        │   ├── ProducerView.java
        │   └── ProducerService.java
        └── consumer/
            ├── ConsumerView.java
            ├── UploadService.java
            ├── MediaService.java
            └── MediaRecord.java
```

## 3. Compile the application

Open a CLI terminal in the folder containing `pom.xml`:

```bash
cd path/to/networked-media
mvn clean compile
```

Maven generates Java gRPC classes from `src/main/proto/media.proto` and compiles the application. The expected successful result is `BUILD SUCCESS`.

To run available automated tests:

```bash
mvn test
```


## 4. Run Consumer (VM 1)

Start the **Consumer first**. In the Consumer VM's terminal, inside the project folder:

```bash
mvn javafx:run
```

This launches `media.consumer.ConsumerView`, the default main class in the complete project's `pom.xml`.

Fill in the Consumer GUI:

| Field | Example | Meaning |
| --- | --- | --- |
| Port | `50051` | gRPC listening port |
| `c` | `2` | Number of concurrent Consumer worker threads |
| `q` | `5` | Maximum number of admitted uploads |
| Save to | `uploads` | Single folder for received videos |

Click **Start Consumer**. Leave this window running while the Producer sends files. Files are saved into the Consumer VM's chosen folder. The GUI lists videos and reports queue and save events.

To find the Consumer VM's IP address:

- Windows: `ipconfig`
- Linux: `ip addr`

The Producer must use this IP address when the applications run on different VMs. Ensure that the VM network settings allow the two machines to reach one another and that the Consumer firewall allows incoming TCP `50051`.

## 5. Run Producer (VM 2)

In the Producer VM's terminal, inside the project folder:

```bash
mvn javafx:run -Dapp.mainClass=media.producer.ProducerView
```

Fill in the Producer GUI:

| Field | Example | Meaning |
| --- | --- | --- |
| Consumer IP | `192.168.1.10` | Actual IP of the Consumer VM |
| Port | `50051` | Same port used by Consumer |
| `p` | `2` | Number of Producer threads and source folders |

Create **one separate folder per Producer thread**. Example for `p=2`:

```text
video-sources/
├── producer1/
│   ├── video1.mp4
│   └── video2.mp4
└── producer2/
    ├── video3.mp4
    └── video4.mp4
```

In the GUI, click **Add Video Folder** and select `producer1`, then add `producer2`. Select **exactly `p` distinct folders**. Click **Start Uploads**. Each Producer thread reads the videos in its assigned folder and sends them through a gRPC client-streaming connection.

For a quick one-computer test, launch Consumer and Producer in separate terminals and set the Consumer IP to `127.0.0.1`. This tests the program locally but **does not satisfy** the separate-VM deployment requirement.




