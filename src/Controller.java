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
    public ArrayList<Combiner> combiners = new ArrayList<Combiner>();
    public ArrayList<Thread> combiners_threads = new ArrayList<Thread>();
    public Boolean combine_finish = false;

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

            // Check if all mappers have finished and if it's time to start the combiners
            if (map_finsh && all_mappers_finished() && available_mapped_chunks.isEmpty()) {
                System.out.println("Map finished");
                init_combiners();

                while (true){
                    if (!available_mapped_chunks.isEmpty()) {
                        verify_state_combiners();
                    }
                    if (all_combiners_finished()) {
                        System.out.println("Reduce finished");
                        combine_finish = true;
                        break; // Exit the loop once all combiners have finished
                    }
                }
                break;
                // Exit the loop once the combiners have started
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

    void verify_state_combiners(){
        for (Combiner combiner : this.combiners) {
            if (!combiner.is_working) {
                // if the combiner is not working, we need to assign another chunk
                combiner.is_working = true;
                combiner.assign_chunk(this.available_mapped_chunks.remove());
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

    void init_combiners() {
        available_mapped_chunks = txtManager.get_mapper_name();
        
        for (int i = 0; i < 4; i++) {
            Combiner combiner = new Combiner(i, this.available_mapped_chunks.remove());
            this.combiners.add(combiner);
            Thread combinerThread = new Thread(combiner);
            this.combiners_threads.add(combinerThread);
            System.out.println("Combiner " + i + " started");
            combinerThread.start();
        }
    }

    boolean all_combiners_finished() {
        for (Thread combinerThread : this.combiners_threads) {
            if (combinerThread.isAlive()) {
                return false;
            }
        }
        return true;
    }

}