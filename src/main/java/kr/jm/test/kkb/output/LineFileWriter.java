package kr.jm.test.kkb.output;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LineFileWriter implements AutoCloseable {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory
            .getLogger(LineFileWriter.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private Writer writer;
    private Path filePath;

    public LineFileWriter(Path filePath) {
        this.filePath = filePath;
        this.writer = buildBufferedAppendWriter(this.filePath
        );
        log.info("LineFileWriter({})", filePath.toAbsolutePath());
    }

    private Writer buildBufferedAppendWriter(Path path) {
        if (!Files.exists(path))
            throw new RuntimeException("No File !!! - " + path);
        try {
            return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption
                    .APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void appendLine(String line) {
        try {
            writer.append(line).append(LINE_SEPARATOR);
        } catch (IOException e) {
            log.error("appendLine({})", line, e);
        }
    }

    public Path closeAndGetFilePath() {
        close();
        return filePath;
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
