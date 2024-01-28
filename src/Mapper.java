import java.util.ArrayList;
import java.io.*;

import static java.lang.Thread.sleep;

public class Mapper implements Runnable {
    int id;
    TxtManager txtManager = new TxtManager();
    String chunk_name;
    String chunk_path;
    boolean it_fails = false;
    boolean is_working = true;
    String mapper_output_path = txtManager.getMapper_output_path();

    public Mapper(int id,String chunk_name, boolean it_fails){
        this.id = id;
        this.chunk_name = chunk_name;
        this.it_fails = it_fails;
        this.chunk_path = txtManager.getChunks_path() + chunk_name;
    }


    @Override
    public void run() {
        while (true){
            if (it_fails || !this.is_working){
                break;
            }
            else {
                ArrayList<String> words_mapped = mapping(chunk_path);
                this.save_mapped_words_to_file(words_mapped, this.mapper_output_path+"mapped_"+chunk_name);
                this.is_working = false;
                System.out.println("Mapper "+ id +" "+chunk_name+" finished");
            }
            
        }
        

    }

    public ArrayList<String> mapping(String chunk_path) {
        try {
            File file = new File(chunk_path);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            String single_line;
            ArrayList<String> words = new ArrayList<String>();

            while ((single_line = br.readLine()) != null) {
                if (single_line.equals("")) {
                    continue;
                }
                //split the line into words and save to an ArrayList
                String[] lineWords = single_line.split(" ");
                for (String word : lineWords) {
                    words.add(word+" 1");
                }
            }
            br.close();
            fr.close();
            return words;

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
           return null;
    }

    public void save_mapped_words_to_file(ArrayList<String> words, String output_file_path) {
        try {
            BufferedWriter bw_words = new BufferedWriter(new FileWriter(output_file_path));
            BufferedWriter bw_mapped_chunk_names = new BufferedWriter(new FileWriter(txtManager.getChunks_name_path()+"mapped_chunk_names.txt"));
            for (String word : words) {
                bw_words.write(word);
                bw_words.newLine();
            }
            bw_mapped_chunk_names.write(chunk_name);
            bw_mapped_chunk_names.newLine();
            bw_mapped_chunk_names.close();
            bw_words.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void restart_mapper(){
        this.it_fails = false;
    }

    public boolean get_status(){
        return this.is_working;
    }

    public void assign_chunk(String chunk_name){
        this.chunk_name = chunk_name;
        this.chunk_path = txtManager.getChunks_path() + chunk_name;
        this.is_working = true;
    }
}
