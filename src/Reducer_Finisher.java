import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Reducer_Finisher implements Runnable {
    private final List<String> combinersOutputFiles;
    private final String outputFile;

    public boolean is_working = true;

    public Reducer_Finisher(List<String> combinersOutputFiles, String outputFile) {
        this.combinersOutputFiles = combinersOutputFiles;
        this.outputFile = outputFile;
    }

    @Override
    public void run() {
        reducer();
        sortFile();
    }

    private void reducer(){
        System.out.println("Reducer_Finisher started");
        Map<String, Integer> wordCounts = new HashMap<>();

        for (String fileName : combinersOutputFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                }
                System.out.println("Reducer_Finisher finished reading file: " + fileName);
            } catch (IOException e) {
                System.out.println("Error reading file: " + fileName);
                e.printStackTrace();
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error writing to file: " + outputFile);
            e.printStackTrace();
        }
        is_working = false;
        System.out.println("Reducer_Finisher finished");
    }


    private void sortFile(){
        try {
            File file = new File(outputFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String single_line;
            Map<String, Integer> wordCounts = new HashMap<>();
            while ((single_line = br.readLine()) != null) {
                String[] parts = single_line.split("\\s+");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
            }
            br.close();
            fr.close();
            wordCounts = wordCounts.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new));
            BufferedWriter bw_words = new BufferedWriter(new FileWriter(outputFile));
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                bw_words.write(entry.getKey() + " " + entry.getValue());
                bw_words.newLine();
            }
            bw_words.close();

        } catch (IOException e) {
            System.out.println(e.getMessage());

    }

    }

}