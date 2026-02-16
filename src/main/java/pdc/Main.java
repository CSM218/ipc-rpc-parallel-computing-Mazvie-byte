package pdc;

import java.io.IOException;

/**
 * Main entry point for running the distributed system standalone.
 * 
 * Usage:
 *   java pdc.Main master <port> <worker_count> <matrix_size>
 *   java pdc.Main worker <master_host> <master_port>
 */
public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String mode = args[0].toLowerCase();

        try {
            switch (mode) {
                case "master":
                    runMaster(args);
                    break;
                case "worker":
                    runWorker(args);
                    break;
                default:
                    System.err.println("Invalid mode: " + mode);
                    printUsage();
                    System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runMaster(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("Usage: java pdc.Main master <port> <worker_count> <matrix_size>");
            return;
        }

        int port = Integer.parseInt(args[1]);
        int workerCount = Integer.parseInt(args[2]);
        int size = Integer.parseInt(args[3]);

        System.out.println("Starting Master on port " + port);
        System.out.println("Expecting " + workerCount + " workers");
        System.out.println("Matrix size: " + size + "x" + size);

        Master master = new Master();
        master.listen(port);

        // Generate data
        System.out.println("Generating matrices...");
        int[][] data = MatrixGenerator.generateRandomMatrix(size, size, 10);
        
        System.out.println("Starting coordination...");
        long start = System.currentTimeMillis();
        
        // Block until complete
        Object result = master.coordinate("MULTIPLY", data, workerCount);
        
        long end = System.currentTimeMillis();
        
        if (result != null) {
            System.out.println("Computation complete in " + (end - start) + "ms");
            int[][] resMatrix = (int[][]) result;
            System.out.println("Result dimensions: " + resMatrix.length + "x" + resMatrix[0].length);
            // Optional: Print small part of result
            System.out.println("Top-left element: " + resMatrix[0][0]);
        } else {
            System.err.println("Computation failed or timed out.");
        }
        
        // Keep alive for a bit to ensure cleanup if needed, or exit
        System.exit(0);
    }

    private static void runWorker(String[] args) {
         if (args.length < 3) {
            System.err.println("Usage: java pdc.Main worker <master_host> <master_port>");
            return;
        }

        String host = args[1];
        int port = Integer.parseInt(args[2]);

        System.out.println("Starting Worker, connecting to " + host + ":" + port);
        new Worker().joinCluster(host, port);
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java pdc.Main master <port> <worker_count> <matrix_size>");
        System.out.println("  java pdc.Main worker <master_host> <master_port>");
    }
}
