import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Combiner implements Runnable {

    private int id;
    private TxtManager txtManager = new TxtManager();
    private String chunk_name;
    private String combinerOutputPath = txtManager.getCombiner_output_path();
    private String mapperOutputPath;

    boolean it_fails = false;
    boolean is_working = true;

    private Map<String, Integer> wordCounts = new HashMap<>();

    public Combiner(int id, String chunkName) {
        this.id = id;
        this.chunk_name = chunkName;
        this.mapperOutputPath = txtManager.getMapper_output_path() + chunkName;
    }

    @Override
    public void run() {
        while (true) {
            if (it_fails || !this.is_working) {
                break;
            } else {
                wordCounts.clear();
                combine();
                save_combined_mapped_chunks_file(wordCounts, combinerOutputPath + "combined_" + chunk_name);
                this.is_working = false;
                System.out.println("Combiner " + id + " " + chunk_name + " finished");
            }
        }
    }

    private void combine() {
        try (BufferedReader reader = new BufferedReader(new FileReader(mapperOutputPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    public void save_combined_mapped_chunks_file(Map<String, Integer> wordCounts, String output_file_path) {
        try {
            BufferedWriter bw_words = new BufferedWriter(new FileWriter(output_file_path));
            BufferedWriter bw_combined_chunk_names = new BufferedWriter(
                    new FileWriter(combinerOutputPath + "combined_chunk_names.txt", true));
            //Sort in alphabetical order
            wordCounts = wordCounts.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new));
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                bw_words.write(entry.getKey() + " " + entry.getValue());
                bw_words.newLine();
            }
            bw_combined_chunk_names.write("combined_" + chunk_name); 
                                                                  
            bw_combined_chunk_names.newLine();
            bw_combined_chunk_names.close();
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

    public void assign_chunk(String chunk_name) {
        this.chunk_name = chunk_name;
        this.mapperOutputPath = txtManager.getMapper_output_path() + chunk_name;
        this.is_working = true;
    }

}