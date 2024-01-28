import java.sql.Struct;
import java.util.ArrayList;
import java.util.Queue;

public class Controller implements Runnable{
    public  TxtManager txtManager = new TxtManager();
    public Queue<String> available_chunks;
    public Queue<String> available_mapped_chunks;
    public ArrayList<Mapper> mappers = new ArrayList<Mapper>();




    public Controller(){
        this.available_chunks = txtManager.get_chunks_name();
    }


    @Override
    public void run() {
        init_mappers();

        while (!available_chunks.isEmpty() ){
            verify_state_mappers();
        }

        while (true){
            if (all_mappers_finished()){
                break;
            }
            break;
        }
        init_reducers();







    }


    void init_mappers(){
        //init four mappers
        for (int i = 0; i < 4; i++) {
            Mapper mapper = new Mapper(i,this.available_chunks.remove(), false);
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

    boolean all_mappers_finished(){
        ArrayList<Boolean> all_mappers_finished = new ArrayList<Boolean>();
        for (Mapper mapper : this.mappers) {
            if (mapper.is_working){
                all_mappers_finished.add(true);
            }
            else {
                all_mappers_finished.add(false);
            }
        }
        if (all_mappers_finished.contains(true)){
            return true;
        }
        return false;
    }

    void init_reducers(){
        available_mapped_chunks = txtManager.get_mapper_name();
        System.out.println(available_mapped_chunks);
    }


}
