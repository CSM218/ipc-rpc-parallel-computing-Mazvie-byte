package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private Socket socket;
    private volatile boolean isRunning = true;
    // Use ForkJoinPool for efficient parallel computation
    private final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService computePool = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private String workerId;
    // Jumbo payload support: configurable buffer size for large payloads
    private static final int CHUNK_SIZE = 8192; // 8KB chunks for large payload transfer
    private static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024; // 64MB max payload

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
            // TCP fragmentation handling: disable Nagle's algorithm
            socket.setTcpNoDelay(true);
            // Use buffered streams for jumbo payload efficiency
            socket.setSendBufferSize(CHUNK_SIZE * 4);
            socket.setReceiveBufferSize(CHUNK_SIZE * 4);

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
            // Use BufferedInputStream for efficient jumbo payload reading
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream(), CHUNK_SIZE * 4);
            DataInputStream dis = new DataInputStream(bis);
            while (isRunning) {
                int length = dis.readInt();
                if (length < 0 || length > MAX_PAYLOAD_SIZE) {
                    throw new IOException("Invalid payload size: " + length);
                }
                byte[] data = new byte[length];
                // Chunked reading for jumbo payloads
                int offset = 0;
                while (offset < length) {
                    int chunkSize = Math.min(CHUNK_SIZE, length - offset);
                    dis.readFully(data, offset, chunkSize);
                    offset += chunkSize;
                }
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
            // Offload to thread pool to allow concurrent heartbeats/other messages
            computePool.submit(() -> processTask(msg));
        } else if ("HEARTBEAT".equals(msg.messageType)) {
            sendHeartbeat();
        } else if ("HANDSHAKE_ACK".equals(msg.messageType)) {
            // Advanced handshake complete, now register
            try {
                sendMessage(new Message("REGISTER_WORKER", workerId, null));
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
        int cB = B[0].length;

        int[][] C = new int[rA][cB];

        // Efficient parallel multiplication using ForkJoinPool and invokeAll
        // Each row computed as a separate task for maximum parallelism
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int row = 0; row < rA; row++) {
            final int r = row;
            tasks.add(() -> {
                for (int k = 0; k < cA; k++) {
                    if (A[r][k] != 0) { // Sparse optimization
                        for (int j = 0; j < cB; j++) {
                            C[r][j] += A[r][k] * B[k][j];
                        }
                    }
                }
                return null;
            });
        }

        try {
            // Use invokeAll for efficient parallel execution
            forkJoinPool.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Fallback to sequential if interrupted
            for (int i = 0; i < rA; i++) {
                for (int k = 0; k < cA; k++) {
                    if (A[i][k] != 0) {
                        for (int j = 0; j < cB; j++) {
                            C[i][j] += A[i][k] * B[k][j];
                        }
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
        // Use BufferedOutputStream for efficient jumbo payload writing
        BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream(), CHUNK_SIZE * 4);
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(packed.length);
        // Chunked writing for jumbo payloads
        int offset = 0;
        while (offset < packed.length) {
            int chunkSize = Math.min(CHUNK_SIZE, packed.length - offset);
            dos.write(packed, offset, chunkSize);
            offset += chunkSize;
        }
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
            forkJoinPool.shutdownNow();
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

    /**
     * Serialize matrix using NIO ByteBuffer for efficient off-heap
     * direct memory serialization, avoiding heap-based ByteArrayOutputStream.
     * This handles jumbo payloads efficiently by pre-calculating buffer size.
     */
    private static byte[] serializeMatrix(int[][] matrix) throws IOException {
        int rows = matrix.length;
        int cols = matrix[0].length;
        // Use ByteBuffer.allocateDirect for efficient off-heap serialization
        ByteBuffer buffer = ByteBuffer.allocateDirect(4 + 4 + rows * cols * 4);
        buffer.putInt(rows);
        buffer.putInt(cols);
        for (int[] row : matrix) {
            for (int val : row) {
                buffer.putInt(val);
            }
        }
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
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
