package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

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

        dos.writeInt(payload.length);
        dos.write(payload);

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

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
        msg.payload = new byte[payloadLen];
        dis.readFully(msg.payload);

        return msg;
    }
}
