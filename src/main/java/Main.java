import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final int NUM_THREADS = 4; // Number of threads for parallel processing
    private static final int CHUNK_SIZE = 1000; // Size of each file chunk

    public static void main(String[] args) throws IOException, InterruptedException {
        String filePath1 = "/path/to/input/file1.csv";
        String filePath2 = "/path/to/input/file2.csv";
        String outputFile = "/path/to/output/reconciled_output.csv";

        reconcileFiles(filePath1, filePath2, outputFile);
    }

    private static void reconcileFiles(String filePath1, String filePath2, String outputFile) throws IOException, InterruptedException {
        // Partition the input files into chunks
        List<List<String[]>> chunks1 = partitionFile(filePath1);
        List<List<String[]>> chunks2 = partitionFile(filePath2);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Future<List<String[]>>> reconciliationTasks = new ArrayList<>();

        // Submit reconciliation tasks for each chunk
        for (int i = 0; i < Math.min(chunks1.size(), chunks2.size()); i++) {
            List<String[]> chunk1 = chunks1.get(i);
            List<String[]> chunk2 = chunks2.get(i);
            reconciliationTasks.add(executor.submit(() -> reconcileChunks(chunk1, chunk2)));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        // Combine the reconciled results from all tasks
        List<String[]> reconciledData = new ArrayList<>();
        for (Future<List<String[]>> task : reconciliationTasks) {
            reconciledData.addAll(task.get());
        }

        // Write the reconciled data to the output file
        writeReconciledData(reconciledData, outputFile);
    }

    private static List<String[]> reconcileChunks(List<String[]> chunk1, List<String[]> chunk2) {
        Map<String, String[]> map1 = new HashMap<>();
        Map<String, String[]> map2 = new HashMap<>();
        List<String[]> reconciledChunk = new ArrayList<>();

        // Populate hash maps for chunk1 and chunk2
        for (String[] record : chunk1) {
            map1.put(record[0], record);
        }
        for (String[] record : chunk2) {
            map2.put(record[0], record);
        }

        // Reconcile data between the two chunks
        for (String key : map1.keySet()) {
            String[] record1 = map1.get(key);
            String[] record2 = map2.remove(key); // Remove to avoid duplicate processing
            if (record2 != null) {
                // Merge records from both files
                reconciledChunk.add(mergeRecords(key, record1, record2));
            } else {
                // Record exists only in file 1
                reconciledChunk.add(markMissingFields(record1, "(f2)"));
            }
        }

        // Process remaining records in file 2
        for (String key : map2.keySet()) {
            String[] record2 = map2.get(key);
            reconciledChunk.add(markMissingFields(record2, "(f1)"));
        }

        return reconciledChunk;
    }

    private static String[] mergeRecords(String key, String[] record1, String[] record2) {
        String[] mergedRecord = new String[record1.length];
        mergedRecord[0] = key; // Key remains the same

        for (int i = 1; i < record1.length; i++) {
            if (record1[i] != null && record2[i] != null) {
                mergedRecord[i] = record1[i]; // Prefer value from file 1
            } else if (record1[i] == null) {
                mergedRecord[i] = record2[i] + "(f1)";
            } else {
                mergedRecord[i] = record1[i] + "(f2)";
            }
        }
        return mergedRecord;
    }

    private static String[] markMissingFields(String[] record, String missingIndicator) {
        String[] markedRecord = new String[record.length];
        markedRecord[0] = record[0]; // Key remains the same

        for (int i = 1; i < record.length; i++) {
            if (record[i] == null) {
                markedRecord[i] = missingIndicator;
            } else {
                markedRecord[i] = record[i];
            }
        }
        return markedRecord;
    }

    private static List<List<String[]>> partitionFile(String filePath) throws IOException {
        List<List<String[]>> chunks = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            List<String[]> currentChunk = new ArrayList<>();
            String line;

            while ((line = br.readLine()) != null) {
                currentChunk.add(line.split(","));
                if (currentChunk.size() >= CHUNK_SIZE) {
                    chunks.add(new ArrayList<>(currentChunk));
                    currentChunk.clear();
                }
            }

            if (!currentChunk.isEmpty()) {
                chunks.add(new ArrayList<>(currentChunk));
            }
        }
        return chunks;
    }

    private static void writeReconciledData(List<String[]> reconciledData, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (String[] record : reconciledData) {
                writer.write(String.join(",", record));
                writer.newLine();
            }
        }
    }
}
