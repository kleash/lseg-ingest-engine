package com.lseg.ingest.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streams the first text-file entry of a .txt.zip as a BufferedReader.
 * Caller must close().
 */
public class ZipLineReader implements AutoCloseable {

    private final InputStream fileStream;
    private final ZipInputStream zip;
    private final BufferedReader reader;

    public ZipLineReader(Path path) throws IOException {
        this.fileStream = Files.newInputStream(path);
        this.zip = new ZipInputStream(fileStream);
        ZipEntry entry = zip.getNextEntry();
        while (entry != null && entry.isDirectory()) entry = zip.getNextEntry();
        if (entry == null) {
            zip.close();
            fileStream.close();
            throw new IOException("Zip is empty: " + path);
        }
        this.reader = new BufferedReader(new InputStreamReader(zip, StandardCharsets.UTF_8), 1 << 16);
    }

    public BufferedReader reader() { return reader; }

    public String readLine() throws IOException { return reader.readLine(); }

    @Override
    public void close() throws IOException {
        try { reader.close(); } finally { try { zip.close(); } finally { fileStream.close(); } }
    }
}
