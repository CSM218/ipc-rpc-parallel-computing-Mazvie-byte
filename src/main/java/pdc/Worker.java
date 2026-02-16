package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private Socket socket;
    private volatile boolean isRunning = true;
    private final ExecutorService computePool = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private String workerId;

    public Worker() {
        this.workerId = "WORKER-" + System.nanoTime(); // Simple ID generation
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            System.out.println("Connecting to master at " + masterHost + ":" + port);
            socket = new Socket(masterHost, port);

            // Send Registration
            sendMessage(new Message("REGISTER_WORKER", workerId, null));

            // Start listening loop
            listenLoop();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // This method might be called by the test harness or main
        // For our implementation, joinCluster starts the loop.
        // If independent execution is needed, we can put logic here.
    }

    private void listenLoop() {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            while (isRunning) {
                int length = dis.readInt();
                byte[] data = new byte[length];
                dis.readFully(data);
                Message msg = Message.unpack(data);
                handleMessage(msg);
            }
        } catch (IOException e) {
            System.err.println("Connection to master lost: " + e.getMessage());
        } finally {
            close();
        }
    }

    private void handleMessage(Message msg) {
        if ("RPC_REQUEST".equals(msg.messageType)) {
            // Offload to thread pool to allow concurrent heartbeats/other messages if
            // needed
            // But here we might want to prioritize computation
            computePool.submit(() -> processTask(msg));
        } else if ("HEARTBEAT".equals(msg.messageType)) {
            sendHeartbeat();
        } else if ("HANDSHAKE_ACK".equals(msg.messageType)) {
            // Handshake complete, now register
            try {
                // Advanced Handshake Start
                sendMessage(new Message("HANDSHAKE_INIT", workerId, null));
                // The original instruction had `listenLoop();IOException e)` here, which is a
                // syntax error.
                // Assuming the intent was to replace the previous `sendMessage` and keep the
                // `try-catch` structure.
                // If `listenLoop()` was intended to be called here, it would need to be outside
                // this `try` block
                // or handled differently to maintain syntactic correctness.
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processTask(Message msg) {
        try {
            // Deserialize payload: [TaskID][MatrixA][MatrixB]
            ByteArrayInputStream bais = new ByteArrayInputStream(msg.payload);
            DataInputStream dis = new DataInputStream(bais);
            String taskId = dis.readUTF();
            int[][] matrixA = deserializeMatrix(dis);
            int[][] matrixB = deserializeMatrix(dis);

            // Perform Multiplication
            int[][] result = multiplyMatrices(matrixA, matrixB);

            // Send Result
            byte[] resultPayload = serializeMatrix(result);
            sendMessage(new Message("TASK_COMPLETE", workerId, resultPayload));

        } catch (IOException e) {
            e.printStackTrace();
            // Ideally send TASK_ERROR
        }
    }

    private int[][] multiplyMatrices(int[][] A, int[][] B) {
        int rA = A.length;
        int cA = A[0].length;
        int rB = B.length; // should equal cA
        int cB = B[0].length;

        int[][] C = new int[rA][cB];
        for (int i = 0; i < rA; i++) {
            for (int k = 0; k < cA; k++) {
                if (A[i][k] != 0) { // Optimization for sparse
                    for (int j = 0; j < cB; j++) {
                        C[i][j] += A[i][k] * B[k][j];
                    }
                }
            }
        }
        return C;
    }

    private void sendHeartbeat() {
        try {
            sendMessage(new Message("HEARTBEAT", workerId, null));
        } catch (IOException e) {
            close();
        }
    }

    private synchronized void sendMessage(Message msg) throws IOException {
        if (socket == null || socket.isClosed())
            return;
        byte[] packed = msg.pack();
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(packed.length);
        dos.write(packed);
        dos.flush();
    }

    public void close() {
        isRunning = false;
        try {
            if (socket != null)
                socket.close();
        } catch (IOException e) {
        }
        try {
            computePool.shutdownNow();
        } catch (Exception e) {
        }
    }

    private static int[][] deserializeMatrix(DataInputStream dis) throws IOException {
        int rows = dis.readInt();
        int cols = dis.readInt();
        int[][] m = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                m[i][j] = dis.readInt();
            }
        }
        return m;
    }

    private static byte[] serializeMatrix(int[][] matrix) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(matrix.length);
        dos.writeInt(matrix[0].length);
        for (int[] row : matrix) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }
        return baos.toByteArray();
    }

    public static void main(String[] args) {
        String host = System.getenv("MASTER_HOST");
        String portStr = System.getenv("MASTER_PORT");
        if (host == null)
            host = "localhost";
        int port = portStr != null ? Integer.parseInt(portStr) : 9999;

        new Worker().joinCluster(host, port);
    }
}
