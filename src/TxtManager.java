import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

public class TxtManager {

    public static int numberOfChunks = 0;

    private String big_file_path = "resources/Texto_Original/Texto_Agrandado/";
    private String chunks_path = "resources/Chunks/";
    private String chunks_name_path = "resources/Chunks/chunks_name.txt";
    private String mapper_output_path = "resources/Mappers_Output/";
    private String reducer_output_path = "resources/Reducers_Output/";
    private String final_output_path = "resources/Final_Output/";

    public TxtManager() {
    }

    // split a document into 32mb chunks
    public void splitFile(String inputFile, int chunkSize) {
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(big_file_path + inputFile))) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkNumber = 1;

            // Create the BufferedWriter outside the loop
            String chunksNamePath = chunks_path + "chunks_name.txt";
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(chunksNamePath))) {

                while ((bytesRead = bis.read(buffer, 0, chunkSize)) > 0) {
                    String chunkName = "chunk" + chunkNumber + ".txt";
                    String outputFilePath = chunks_path + chunkName;

                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFilePath))) {
                        bos.write(buffer, 0, bytesRead);

                        // Write the chunk name to the chunks_name.txt file
                        bw.write(chunkName);
                        bw.newLine();
                    }

                    chunkNumber++;
                    buffer = new byte[chunkSize];
                }
            }

            numberOfChunks = chunkNumber;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Queue<String> get_chunks_name(){
        Queue<String> chunks_name = new ArrayDeque<String>();
        try {
            File file = new File(chunks_name_path);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            String single_line;

            while ((single_line = br.readLine()) != null) {
                chunks_name.add(single_line);
            }
            br.close();
            fr.close();
            return chunks_name;

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;

    }



    public String getBig_file_path() {
        return big_file_path;
    }
    public String getChunks_path() {
        return chunks_path;
    }
    public String getChunks_name_path() {
        return chunks_name_path;
    }
    public String getMapper_output_path() {
        return mapper_output_path;
    }
    public String getReducer_output_path() {
        return reducer_output_path;
    }
    public String getFinal_output_path() {
        return final_output_path;
    }
}

