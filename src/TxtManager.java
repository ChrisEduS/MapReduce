import java.io.*;
import java.util.ArrayList;

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
    public void split_file(String inputFile, int chunkSize) {
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(big_file_path+inputFile))) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkNumber = 1;

            while ((bytesRead = bis.read(buffer, 0, chunkSize)) > 0) {

                String chunk_name = "chunk" + chunkNumber + ".txt";
                String outputFilePath = chunks_path + chunk_name;
                String chunksNamePath = chunks_path + "chunks_name.txt";

                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFilePath));
                     BufferedWriter bw = new BufferedWriter(new FileWriter(chunksNamePath))) {
                    bos.write(buffer, 0, bytesRead);
                    bw.write(chunk_name);
                    bw.newLine();
                }
                chunkNumber++;
                buffer = new byte[chunkSize];
            }
            numberOfChunks = chunkNumber;

        } catch (IOException e) {
            e.printStackTrace();
        }
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

