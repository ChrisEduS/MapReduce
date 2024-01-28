import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Reducer implements Runnable {

    private int id;
    private TxtManager txtManager = new TxtManager();
    private String chunkName;
    private String reducerOutputPath = txtManager.getReducer_output_path();
    private String mapperOutputPath;

    boolean it_fails = false;
    boolean is_working = true;

    private static Map<String, Integer> wordCounts = new ConcurrentHashMap<>();

    public Reducer(int id) {
        this.id = id;
    }

    @Override
    public void run() {
    }

    private void reduce() throws IOException {
        // Check if the file exists and is readable
        File file = new File(this.mapperOutputPath);
        if (!file.exists()) {
            System.out.println("File " + this.mapperOutputPath + " does not exist");
        } else if (!file.canRead()) {
            System.out.println("File " + this.mapperOutputPath + " cannot be read");
        } else {
            System.out.println("Reducer " + id + " " + chunkName + " started");
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(mapperOutputPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                // wordCounts.compute(word, (key, value) -> (value == null) ? count : value +//
                // count);
            }
        }

        // Save the reduced chunk to a file
        save_reduced_mapped_chunks_file(wordCounts, reducerOutputPath + "reduced_" + chunkName);

    }

    public void save_reduced_mapped_chunks_file(Map<String, Integer> wordCounts, String output_file_path) {
        try {
            BufferedWriter bw_words = new BufferedWriter(new FileWriter(output_file_path));
            BufferedWriter bw_reduced_chunk_names = new BufferedWriter(
                    new FileWriter(txtManager.getReducer_output_path() + "reduced_chunk_names.txt", true));
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                bw_words.write(entry.getKey() + " " + entry.getValue());
                bw_words.newLine();
            }
            bw_reduced_chunk_names.write("reduced_" + chunkName); // this is the name of the reduced chunk, for example
                                                                  // "reduced_chunk_1.txt
            bw_reduced_chunk_names.newLine();
            bw_reduced_chunk_names.close();
            bw_words.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public boolean isWorking() {
        return is_working;
    }

    public boolean hasFailed() {
        return it_fails;
    }
}