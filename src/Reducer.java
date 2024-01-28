import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Reducer implements Runnable {

    private int id;
    private TxtManager txtManager = new TxtManager();
    private String chunkName;
    private String mapperOutputPath = txtManager.getReducer_output_path();
    private String reducerOutputPath;

    boolean it_fails = false;
    boolean is_working = true;

    private static Map<String, Integer> wordCounts = new ConcurrentHashMap<>();

    public Reducer(int id,String chunkName) {
        this.id = id;
        this.chunkName = chunkName;
        this.reducerOutputPath = txtManager.getMapper_output_path() + "mapped_"+chunkName;
        System.out.println("Reducer "+ id +" "+chunkName+" started");
    }

    @Override
    public void run() {
        try {
            reduce();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void reduce() throws IOException {
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

        try (PrintWriter writer = new PrintWriter(new FileWriter(reducerOutputPath))) {
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                writer.println(entry.getKey() + " " + entry.getValue());
            }
        }
    }
}