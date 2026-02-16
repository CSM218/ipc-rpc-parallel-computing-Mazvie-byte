package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Custom wire-format message for the distributed computing protocol.
 * 
 * REQUIREMENT: You MUST implement a custom wire format.
 * Using JSON or standard Java Serialization (ObjectOutputStream) is FORBIDDEN.
 * 
 * Supports jumbo payloads via buffered I/O and chunked transfer.
 * Uses efficient binary encoding for high throughput.
 */
public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    // Maximum payload size for jumbo payloads (e.g., large matrices)
    public static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024; // 64MB
    // Chunk size for chunked transfer of large payloads
    public static final int CHUNK_SIZE = 8192;

    public Message() {
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Serialize this message into a byte array using a custom binary protocol.
     * Uses BufferedOutputStream for efficiency with large (jumbo) payloads.
     * Format:
     * [MagicLen][Magic][Version][TypeLen][Type][SenderLen][Sender][Timestamp][PayloadLen][Payload]
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedOutputStream bos = new BufferedOutputStream(baos, CHUNK_SIZE);
        DataOutputStream dos = new DataOutputStream(bos);

        byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(magicBytes.length);
        dos.write(magicBytes);

        dos.writeInt(version);

        byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(typeBytes.length);
        dos.write(typeBytes);

        byte[] senderBytes = studentId.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(senderBytes.length);
        dos.write(senderBytes);

        dos.writeLong(timestamp);

        if (payload == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(payload.length);
            // Write payload in chunks for jumbo payload support
            int offset = 0;
            while (offset < payload.length) {
                int chunkLen = Math.min(CHUNK_SIZE, payload.length - offset);
                dos.write(payload, offset, chunkLen);
                offset += chunkLen;
            }
        }

        dos.flush();
        bos.flush();
        return baos.toByteArray();
    }

    /**
     * Deserialize a byte array into a Message object.
     * Validates the protocol magic number and supports large payloads.
     */
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        BufferedInputStream bis = new BufferedInputStream(bais, CHUNK_SIZE);
        DataInputStream dis = new DataInputStream(bis);

        Message msg = new Message();

        int magicLen = dis.readInt();
        byte[] magicBytes = new byte[magicLen];
        dis.readFully(magicBytes);
        msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

        if (!"CSM218".equals(msg.magic)) {
            throw new IOException("Invalid Magic: " + msg.magic);
        }

        msg.version = dis.readInt();

        int typeLen = dis.readInt();
        byte[] typeBytes = new byte[typeLen];
        dis.readFully(typeBytes);
        msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);

        int senderLen = dis.readInt();
        byte[] senderBytes = new byte[senderLen];
        dis.readFully(senderBytes);
        msg.studentId = new String(senderBytes, StandardCharsets.UTF_8);

        msg.timestamp = dis.readLong();

        int payloadLen = dis.readInt();
        if (payloadLen > MAX_PAYLOAD_SIZE) {
            throw new IOException("Payload too large: " + payloadLen + " bytes (max: " + MAX_PAYLOAD_SIZE + ")");
        }

        // Read payload in chunks for jumbo payload efficiency
        msg.payload = new byte[payloadLen];
        int totalRead = 0;
        while (totalRead < payloadLen) {
            int chunkLen = Math.min(CHUNK_SIZE, payloadLen - totalRead);
            dis.readFully(msg.payload, totalRead, chunkLen);
            totalRead += chunkLen;
        }

        return msg;
    }

    /**
     * Validate this message for protocol compliance.
     */
    public boolean isValid() {
        return "CSM218".equals(magic) && version >= 1 && messageType != null && studentId != null;
    }

    /**
     * Get the total wire size of this message (for efficiency monitoring).
     */
    public int getWireSize() {
        int size = 4; // magic length prefix
        size += magic.getBytes(StandardCharsets.UTF_8).length;
        size += 4; // version
        size += 4 + messageType.getBytes(StandardCharsets.UTF_8).length;
        size += 4 + studentId.getBytes(StandardCharsets.UTF_8).length;
        size += 8; // timestamp
        size += 4 + (payload != null ? payload.length : 0);
        return size;
    }

    @Override
    public String toString() {
        return "Message{type=" + messageType + ", from=" + studentId
                + ", payloadSize=" + (payload != null ? payload.length : 0) + "}";
    }
}
