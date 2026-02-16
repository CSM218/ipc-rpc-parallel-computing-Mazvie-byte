package pdc;

import java.util.Arrays;

public class MessageTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Message Packing and Unpacking...");

            String type = "TEST_TYPE";
            String sender = "TEST_SENDER";
            byte[] payload = "Hello World".getBytes();

            Message original = new Message(type, sender, payload);

            byte[] packed = original.pack();
            System.out.println("Packed size: " + packed.length + " bytes");

            Message unpacked = Message.unpack(packed);

            if (!original.magic.equals(unpacked.magic))
                throw new RuntimeException("Magic mismatch");
            if (original.version != unpacked.version)
                throw new RuntimeException("Version mismatch");
            if (!original.messageType.equals(unpacked.messageType))
                throw new RuntimeException("Type mismatch");
            if (!original.studentId.equals(unpacked.studentId))
                throw new RuntimeException("Sender mismatch");
            if (original.timestamp != unpacked.timestamp)
                throw new RuntimeException("Timestamp mismatch");
            if (!Arrays.equals(original.payload, unpacked.payload))
                throw new RuntimeException("Payload mismatch");

            System.out.println("SUCCESS: Message serialization verified!");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
