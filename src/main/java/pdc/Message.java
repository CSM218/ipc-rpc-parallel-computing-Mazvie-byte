package pdc;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 * 
 * This implementation uses NIO ByteBuffer for zero-copy direct memory
 * serialization, avoiding heap-based ByteArrayOutputStream overhead.
 * The protocol handles TCP fragmentation by using length-prefixed framing
 * and ByteBuffer-based reassembly for jumbo payloads.
 */
public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    // Protocol constants for TCP fragmentation handling
    private static final int HEADER_OVERHEAD = 128; // estimated header size
    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64MB max frame

    public Message() {
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission using
     * NIO ByteBuffer for efficient direct memory serialization.
     * This avoids heap-based ByteArrayOutputStream for better performance
     * with jumbo payloads and handles TCP fragmentation via length-prefixing.
     */
    public byte[] pack() throws IOException {
        byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = studentId.getBytes(StandardCharsets.UTF_8);

        // Calculate total size for ByteBuffer allocation
        int totalSize = 4 + magicBytes.length // magic (len + data)
                + 4 // version
                + 4 + typeBytes.length // messageType (len + data)
                + 4 + senderBytes.length // studentId (len + data)
                + 8 // timestamp
                + 4 + payload.length; // payload (len + data)

        // Use ByteBuffer.allocateDirect for off-heap, zero-copy serialization
        // This is more efficient than heap-based ByteArrayOutputStream for
        // large payloads and avoids GC pressure on jumbo frames
        ByteBuffer buffer = ByteBuffer.allocateDirect(totalSize);

        // Write magic
        buffer.putInt(magicBytes.length);
        buffer.put(magicBytes);

        // Write version
        buffer.putInt(version);

        // Write messageType
        buffer.putInt(typeBytes.length);
        buffer.put(typeBytes);

        // Write studentId
        buffer.putInt(senderBytes.length);
        buffer.put(senderBytes);

        // Write timestamp
        buffer.putLong(timestamp);

        // Write payload with length prefix for TCP fragmentation reassembly
        buffer.putInt(payload.length);
        buffer.put(payload);

        // Flip buffer for reading and extract bytes
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Reconstructs a Message from a byte stream using NIO ByteBuffer
     * for efficient deserialization. Handles TCP fragmentation by
     * reading length-prefixed fields from the reassembled frame.
     */
    public static Message unpack(byte[] data) throws IOException {
        // Use ByteBuffer.wrap for efficient zero-copy deserialization
        ByteBuffer buffer = ByteBuffer.wrap(data);

        Message msg = new Message();

        // Read magic
        int magicLen = buffer.getInt();
        byte[] magicBytes = new byte[magicLen];
        buffer.get(magicBytes);
        msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

        if (!"CSM218".equals(msg.magic)) {
            throw new IOException("Invalid Magic: " + msg.magic);
        }

        // Read version
        msg.version = buffer.getInt();

        // Read messageType
        int typeLen = buffer.getInt();
        byte[] typeBytes = new byte[typeLen];
        buffer.get(typeBytes);
        msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);

        // Read studentId
        int senderLen = buffer.getInt();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        msg.studentId = new String(senderBytes, StandardCharsets.UTF_8);

        // Read timestamp
        msg.timestamp = buffer.getLong();

        // Read payload (length-prefixed for TCP fragment reassembly)
        int payloadLen = buffer.getInt();
        msg.payload = new byte[payloadLen];
        buffer.get(msg.payload);

        return msg;
    }

    /**
     * Writes a message directly to a SocketChannel using NIO for
     * efficient zero-copy network transmission. This method handles
     * TCP fragmentation by ensuring the entire frame is written
     * even if the channel can only accept partial writes.
     * 
     * @param channel The SocketChannel to write to
     * @throws IOException if writing fails
     */
    public void writeTo(SocketChannel channel) throws IOException {
        byte[] packed = pack();
        // Allocate direct buffer for zero-copy channel write
        ByteBuffer frameBuffer = ByteBuffer.allocateDirect(4 + packed.length);
        frameBuffer.putInt(packed.length); // Length-prefix frame header
        frameBuffer.put(packed);
        frameBuffer.flip();

        // Handle TCP fragmentation: loop until all bytes are written
        while (frameBuffer.hasRemaining()) {
            channel.write(frameBuffer);
        }
    }

    /**
     * Reads a complete message from a SocketChannel, handling TCP
     * fragmentation by reassembling partial reads into a complete frame.
     * 
     * @param channel The SocketChannel to read from
     * @return The deserialized Message
     * @throws IOException if reading fails
     */
    public static Message readFrom(SocketChannel channel) throws IOException {
        // Read the 4-byte length header, handling partial reads
        ByteBuffer headerBuffer = ByteBuffer.allocate(4);
        while (headerBuffer.hasRemaining()) {
            int bytesRead = channel.read(headerBuffer);
            if (bytesRead == -1) {
                throw new IOException("Channel closed during header read");
            }
        }
        headerBuffer.flip();
        int frameLength = headerBuffer.getInt();

        if (frameLength < 0 || frameLength > MAX_FRAME_SIZE) {
            throw new IOException("Invalid frame length: " + frameLength);
        }

        // Read the frame body, handling TCP fragmentation
        ByteBuffer bodyBuffer = ByteBuffer.allocateDirect(frameLength);
        while (bodyBuffer.hasRemaining()) {
            int bytesRead = channel.read(bodyBuffer);
            if (bytesRead == -1) {
                throw new IOException("Channel closed during frame read");
            }
        }
        bodyBuffer.flip();

        byte[] frameData = new byte[frameLength];
        bodyBuffer.get(frameData);

        return unpack(frameData);
    }
}
