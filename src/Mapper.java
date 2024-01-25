import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class Mapper implements Runnable {
    public ArrayList<String>   chunks;

    String chunk_path;
    public Mapper(ArrayList<String> chunks){
        this.chunks = chunks;
    }


    @Override
    public void run() {
       // ArrayList<String> chunk = readAndSplit(this.chunk_path);
        try {
            sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Mapper terminado");
        System.out.println("Chunks: " + chunks);

    }


    public ArrayList<String> readAndSplit(String chunk_path){
        TxtManager txtManager = new TxtManager();
        ArrayList<String>palabras = txtManager.leerArchivo(chunk_path);
        return palabras;
    }

    public void setChunks(ArrayList<String> chunks){
        this.chunks = chunks;
    }

    public synchronized void addChunk(String chunk){
        this.chunks.add(chunk);
        System.out.println("Chunk agregado: " + chunk);
    }



}
