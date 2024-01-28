import java.util.ArrayList;
import java.util.Queue;
import java.util.Scanner;

public class Controller implements Runnable{
    public  TxtManager txtManager = new TxtManager();
    public Queue<String> available_chunks;

    public ArrayList<Mapper> mappers = new ArrayList<Mapper>();



    public Controller(){
        this.available_chunks = txtManager.get_chunks_name();
    }


    @Override
    public void run() {
        init_mappers();

        while (!available_chunks.isEmpty()) {
            verify_state_mappers();
        }




    }


    void init_mappers(){
        //init four mappers
        for (int i = 0; i < 4; i++) {
            Mapper mapper = new Mapper(i+1,this.available_chunks.remove(), false);
            this.mappers.add(mapper);
            Thread mapperThread = new Thread(mapper);
            mapperThread.start();
            System.out.println("Mapper "+i+" started");
        }
    }

    void verify_state_mappers(){
        for (Mapper mapper : this.mappers) {
            if (!mapper.is_working){
                //if the mapper is not working, we need to assign another chunk
                mapper.is_working = true;
                mapper.assign_chunk(this.available_chunks.remove());
            }
        }
    }


}
