import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ReconciliationApplication {
    private static final int NUM_THREADS = 4; 

    public static void main(String[] args) throws IOException, InterruptedException {
        String filePath1 = "/path/to/input/file1.csv";
        String filePath2 = "/path/to/input/file2.csv";

        String outputFile = "/path/to/output/reconciled_output.csv";

        reconcileFiles(filePath1, filePath2, outputFile);
    }

    private static void reconcileFiles(String filePath1, String filePath2, String outputFile) throws IOException, InterruptedException {
        List<List<String[]>> chunks1 = partitionFile(filePath1);
        List<List<String[]>> chunks2 = partitionFile(filePath2);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        List<Future<List<String[]>>> reconciliationTasks = new ArrayList<>();

        for (int i = 0; i < chunks1.size(); i++) {
            List<String[]> chunk1 = chunks1.get(i);
            List<String[]> chunk2 = chunks2.get(i);
            reconciliationTasks.add(executor.submit(() -> reconcileChunks(chunk1, chunk2)));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        List<String[]> reconciledData = new ArrayList<>();
        for (Future<List<String[]>> task : reconciliationTasks) {
            reconciledData.addAll(task.get());
        }

        writeReconciledData(reconciledData, outputFile);
    }

    private static List<String[]> reconcileChunks(List<String[]> chunk1, List<String[]> chunk2) {
        return new ArrayList<>(chunk1);
    }

    private static List<List<String[]>> partitionFile(String filePath) throws IOException {
      
        List<List<String[]>> chunks = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            List<String[]> currentChunk = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                currentChunk.add(line.split(","));
                if (currentChunk.size() >= 1000) { 
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
