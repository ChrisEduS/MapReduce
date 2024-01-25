import java.util.ArrayList;

public class Mapper implements Runnable {

    String chunk_path;
    public Mapper(String chunk_path){
        this.chunk_path = chunk_path;
    }


    @Override
    public void run() {
        ArrayList<String> chunk = readAndSplit(this.chunk_path);

    }


    public ArrayList<String> readAndSplit(String chunk_path){
        TxtManager txtManager = new TxtManager();
        ArrayList<String>palabras = txtManager.leerArchivo(chunk_path);
        return palabras;
    }



}
