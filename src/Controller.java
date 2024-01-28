import java.sql.Struct;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class Controller implements Runnable {
    public TxtManager txtManager = new TxtManager();
    public Queue<String> available_chunks;
    public Queue<String> available_mapped_chunks = new LinkedList<String>();
    public ArrayList<Mapper> mappers = new ArrayList<Mapper>();

    public ArrayList<Thread> mappers_threads = new ArrayList<Thread>();
    public Boolean map_finsh = false;
    public ArrayList<Reducer> reducers = new ArrayList<Reducer>();
    public ArrayList<Thread> reducers_threads = new ArrayList<Thread>();
    public Boolean reduce_finish = false;

    public Controller() {
        this.available_chunks = txtManager.get_chunks_name();
    }

    @Override
    public void run() {
        init_mappers();

        while (true) {
            if (!available_chunks.isEmpty()) {
                verify_state_mappers();
                if (available_chunks.size() == 0) {
                    map_finsh = true;
                }
            }

            // Check if all mappers have finished and if it's time to start the reducers
            if (map_finsh && all_mappers_finished() && available_mapped_chunks.isEmpty()) {
                System.out.println("Map finished");
                init_reducers();
                if (!available_mapped_chunks.isEmpty()) {
                    verify_state_reducers();
                }
                if (all_reducers_finished()) {
                    System.out.println("Reduce finished");
                    reduce_finish = true;
                    break; // Exit the loop once all reducers have finished
                }
                // Exit the loop once the reducers have started
            }

        }

    }

    void init_mappers() {
        // init four mappers
        for (int i = 0; i < 4; i++) {
            Mapper mapper = new Mapper(i, this.available_chunks.remove(), false);
            this.mappers.add(mapper);
            Thread mapperThread = new Thread(mapper);
            this.mappers_threads.add(mapperThread);
            mapperThread.start();
            System.out.println("Mapper " + i + " started");
        }
    }

    void verify_state_mappers() {
        for (Mapper mapper : this.mappers) {
            if (!mapper.is_working) {
                // if the mapper is not working, we need to assign another chunk
                mapper.is_working = true;
                mapper.assign_chunk(this.available_chunks.remove());
            }
        }
    }

    void verify_state_reducers(){
        for (Reducer reducer : this.reducers) {
            if (!reducer.is_working) {
                // if the reducer is not working, we need to assign another chunk
                reducer.assign_chunk(this.available_mapped_chunks.remove());
            }
        }
    }

    boolean all_mappers_finished() {
        for (Thread mapperThread : this.mappers_threads) {
            if (mapperThread.isAlive()) {
                return false;
            }
        }
        return true;
    }

    void init_reducers() {
        available_mapped_chunks = txtManager.get_mapper_name();
        System.out.println(available_mapped_chunks);
        for (int i = 0; i < 2; i++) {
            Reducer reducer = new Reducer(i, this.available_mapped_chunks.remove());
            this.reducers.add(reducer);
            Thread reducerThread = new Thread(reducer);
            this.reducers_threads.add(reducerThread);
            reducerThread.start();
            System.out.println("Reducer " + i + " started");
        }
    }

    boolean all_reducers_finished() {
        for (Thread reducerThread : this.reducers_threads) {
            if (reducerThread.isAlive()) {
                return false;
            }
        }
        return true;
    }

}