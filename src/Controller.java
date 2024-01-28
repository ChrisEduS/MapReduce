import java.sql.Struct;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class Controller implements Runnable {
    public TxtManager txtManager = new TxtManager();
    public Queue<String> available_chunks;
    public Queue<String> available_mapped_chunks = new LinkedList<>();;
    public ArrayList<Mapper> mappers = new ArrayList<Mapper>();

    public Boolean map_finsh = false;

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
                break; // Exit the loop once the reducers have started
            }
        }

    }

    void init_mappers() {
        // init four mappers
        for (int i = 0; i < 4; i++) {
            Mapper mapper = new Mapper(i, this.available_chunks.remove(), false);
            this.mappers.add(mapper);
            Thread mapperThread = new Thread(mapper);
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

    boolean all_mappers_finished() {
        for (Mapper mapper : this.mappers) {
            if (mapper.is_working) {
                return false; // Return false as soon as we find a mapper that is still working
            }
        }
        return true; // Only return true if we've checked all mappers and none of them are working
    }

    void init_reducers() {
        available_mapped_chunks = txtManager.get_mapper_name();
        System.out.println(available_mapped_chunks);
    }

}