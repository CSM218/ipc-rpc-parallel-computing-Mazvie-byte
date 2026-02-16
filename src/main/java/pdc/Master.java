package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerHandler> workers = new ConcurrentHashMap<>();
    private final Queue<Task> pendingTasks = new ConcurrentLinkedQueue<>();
    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    private int port;

    public Master() {
    }

    /**
     * Shuts down the master, closing the server socket and stopping all threads.
     */
    public void shutdown() {
        isRunning = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Close all worker connections
        for (WorkerHandler wh : workers.values()) {
            wh.close();
        }
        workers.clear();
        systemThreads.shutdownNow(); // Force shutdown of handlers
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation   A string descriptor of the matrix operation (e.g.
     *                    "BLOCK_MULTIPLY")
     * @param data        The raw matrix data to be processed (Matrix A)
     * @param workerCount Expected number of workers
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Wait for workers to join
        long startWait = System.currentTimeMillis();
        while (workers.size() < workerCount) {
            try {
                Thread.sleep(100);
                if (System.currentTimeMillis() - startWait > 30000) {
                    System.err.println("Timeout waiting for workers");
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        if (workers.size() < workerCount) {
            System.err.println("Insufficient workers joined. Aborting.");
            return null;
        }

        System.out.println("Starting coordination with " + workers.size() + " workers");

        // Partition the problem
        int rows = data.length;
        int cols = data[0].length;
        // For simplicity, we assume we are multiplying A x A (since we only have
        // 'data')
        // Or we treat 'data' as A and B.
        // Let's assume we are squaring the matrix A * A for the assignment context
        // unless specified.
        int[][] matrixB = data;

        int rowBlockSize = (int) Math.ceil((double) rows / workers.size());
        if (rowBlockSize == 0)
            rowBlockSize = 1;

        List<Future<int[][]>> results = new ArrayList<>();
        Map<String, Task> tasks = new ConcurrentHashMap<>();

        int taskIdCounter = 0;
        for (int i = 0; i < rows; i += rowBlockSize) {
            int endRow = Math.min(i + rowBlockSize, rows);
            int[][] subMatrixA = Arrays.copyOfRange(data, i, endRow);

            Task task = new Task(String.valueOf(taskIdCounter++), subMatrixA, matrixB, i);
            tasks.put(task.id, task);
            pendingTasks.add(task);
        }

        // Schedule units across dynamic pool
        // We use a completion service or countdown latch.
        // Here we'll dispatch tasks to available workers.

        int totalTasks = tasks.size();
        CountDownLatch latch = new CountDownLatch(totalTasks);
        int[][] resultMatrix = new int[rows][matrixB[0].length];

        // Scheduler thread
        Thread scheduler = new Thread(() -> {
            while (latch.getCount() > 0 && isRunning) {
                Task task = pendingTasks.poll();
                if (task != null) {
                    WorkerHandler worker = getAvailableWorker();
                    if (worker != null) {
                        try {
                            worker.assignTask(task, (result) -> {
                                synchronized (resultMatrix) {
                                    for (int r = 0; r < result.length; r++) {
                                        resultMatrix[task.startRow + r] = result[r];
                                    }
                                }
                                latch.countDown();
                            });
                        } catch (Exception e) {
                            System.err.println("Failed to assign task, retrying/reassigning: " + e.getMessage());
                            pendingTasks.add(task); // Retry logic
                        }
                    } else {
                        pendingTasks.add(task); // No worker available, put back
                        try {
                            Thread.sleep(50);
                        } catch (Exception e) {
                        }
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (Exception e) {
                }
            }
        });
        scheduler.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return resultMatrix;
    }

    private WorkerHandler getAvailableWorker() {
        // Simple round-robin or load based.
        // For now, return any idle worker or just random one if all busy (async).
        // Best to check if worker is assumed 'alive'.
        for (WorkerHandler wh : workers.values()) {
            if (wh.isAlive() && wh.isReady()) {
                return wh;
            }
        }
        // If all busy, pick one randomly? Or wait?
        // For simplicity, pick first alive.
        for (WorkerHandler wh : workers.values()) {
            if (wh.isAlive())
                return wh;
        }
        return null;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        this.port = port;
        serverSocket = new ServerSocket(port);
        System.out.println("Master listening on port " + port);

        Thread acceptor = new Thread(() -> {
            while (isRunning) {
                try {
                    Socket socket = serverSocket.accept();
                    WorkerHandler handler = new WorkerHandler(socket, this);
                    systemThreads.submit(handler);
                } catch (IOException e) {
                    if (isRunning)
                        e.printStackTrace();
                }
            }
        });
        acceptor.start();

        // Start Heartbeat monitor
        startHeartbeatMonitor();
    }

    // Recovery mechanism
    private void startHeartbeatMonitor() {
        Thread monitor = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(2000); // Check every 2 seconds
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, WorkerHandler> entry : workers.entrySet()) {
                        WorkerHandler wh = entry.getValue();
                        if (now - wh.lastHeartbeat > 5000) { // 5 second timeout
                            System.err.println("Worker " + wh.workerId + " timed out. Detecting failure.");
                            wh.close();
                            workers.remove(entry.getKey());
                            // Reassign/Redistribute tasks functionality would happen here
                            // Any task held by 'wh' should be failed and re-added to pendingTasks
                            wh.failActiveTasks();
                        } else {
                            // Send Ping
                            wh.sendHeartbeat();
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // Explicit reconciliation if needed, handled by monitor.
    }

    public void registerWorker(String workerId, WorkerHandler handler) {
        workers.put(workerId, handler);
        System.out.println("Worker registered: " + workerId);
    }

    public void reassignTask(Task task) {
        System.out.println("Reassigning task " + task.id);
        pendingTasks.add(task);
    }

    // Inner classes

    static class Task {
        String id;
        int[][] subA;
        int[][] matrixB;
        int startRow;

        public Task(String id, int[][] subA, int[][] matrixB, int startRow) {
            this.id = id;
            this.subA = subA;
            this.matrixB = matrixB;
            this.startRow = startRow;
        }
    }

    class WorkerHandler implements Runnable {
        private Socket socket;
        private Master master;
        private DataOutputStream out;
        private DataInputStream in; // used via Message.unpack
        private volatile boolean active = true;
        public String workerId;
        public long lastHeartbeat = System.currentTimeMillis();
        private boolean ready = true; // Simple busy flag

        // Current task tracking for recovery
        private Task currentTask;
        private java.util.function.Consumer<int[][]> currentCallback;

        public WorkerHandler(Socket socket, Master master) {
            this.socket = socket;
            this.master = master;
        }

        public boolean isAlive() {
            return active;
        }

        public boolean isReady() {
            return ready;
        }

        @Override
        public void run() {
            try {
                System.out.println("Handler started for " + socket.getInetAddress());
                in = new DataInputStream(socket.getInputStream());
                out = new DataOutputStream(socket.getOutputStream());

                while (active) {
                    int length = in.readInt();
                    byte[] data = new byte[length];
                    in.readFully(data);
                    Message msg = Message.unpack(data);
                    handleMessage(msg);
                }
            } catch (IOException e) {
                // EOF or closed
                close();
            } catch (Exception e) {
                e.printStackTrace();
                close();
            }
        }

        // Revised run() with framing
        public void listenLoop() {
            try {
                java.io.DataInputStream dis = new java.io.DataInputStream(socket.getInputStream());
                while (active) {
                    int length = dis.readInt();
                    byte[] data = new byte[length];
                    dis.readFully(data);
                    Message msg = Message.unpack(data);
                    handleMessage(msg);
                }
            } catch (Exception e) {
                close();
            }
        }

        public void start() {
            new Thread(this::listenLoop).start();
        }

        private void handleMessage(Message msg) {
            lastHeartbeat = System.currentTimeMillis();
            if ("REGISTER_WORKER".equals(msg.messageType)) {
                this.workerId = msg.studentId;
                master.registerWorker(workerId, this);
            } else if ("HANDSHAKE_INIT".equals(msg.messageType)) {
                // Advanced Handshake step 1: Worker says Hello
                // Master responds with HANDSHAKE_ACK
                try {
                    sendMessage(new Message("HANDSHAKE_ACK", "MASTER", null));
                } catch (IOException e) {
                    close();
                }
            } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                // Parse result
                // We need to deserialize the int[][] result from msg.payload
                ready = true;
                if (currentCallback != null) {
                    int[][] result = deserializeMatrix(msg.payload);
                    currentCallback.accept(result);
                    currentCallback = null;
                    currentTask = null;
                }
            } else if ("HEARTBEAT".equals(msg.messageType)) {
                // Heartbeat ack
            }
        }

        public void sendHeartbeat() {
            try {
                sendMessage(new Message("HEARTBEAT", "MASTER", null));
            } catch (IOException e) {
                close();
            }
        }

        public void assignTask(Task task, java.util.function.Consumer<int[][]> callback) throws IOException {
            this.currentTask = task;
            this.currentCallback = callback;
            this.ready = false;
            // Serialize task
            byte[] payload = serializeTask(task);
            sendMessage(new Message("RPC_REQUEST", "MASTER", payload));
        }

        private void sendMessage(Message msg) throws IOException {
            synchronized (socket) {
                byte[] packed = msg.pack();
                java.io.DataOutputStream dos = new java.io.DataOutputStream(socket.getOutputStream());
                dos.writeInt(packed.length);
                dos.write(packed);
                dos.flush();
            }
        }

        public void close() {
            active = false;
            try {
                socket.close();
            } catch (IOException e) {
            }
        }

        public void failActiveTasks() {
            if (currentTask != null) {
                master.reassignTask(currentTask);
                currentTask = null;
                currentCallback = null;
            }
        }
    }

    // Helpers for matrix serialization
    private static byte[] serializeTask(Task t) {
        // [Rows][Cols][...data...] for A
        // [Rows][Cols][...data...] for B
        // [StartRow]
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
            // Task ID (or encoded in message)
            dos.writeUTF(t.id);
            serializeMatrix(dos, t.subA);
            serializeMatrix(dos, t.matrixB);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void serializeMatrix(java.io.DataOutputStream dos, int[][] matrix) throws IOException {
        dos.writeInt(matrix.length);
        dos.writeInt(matrix[0].length);
        for (int[] row : matrix) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }
    }

    private static int[][] deserializeMatrix(byte[] data) {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
            java.io.DataInputStream dis = new java.io.DataInputStream(bais);
            int rows = dis.readInt();
            int cols = dis.readInt();
            int[][] m = new int[rows][cols];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    m[i][j] = dis.readInt();
                }
            }
            return m;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
