import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import static java.lang.Thread.sleep;

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

    public int number_of_mappers;
    public int number_of_combiners;
    public int mappers_fail;
    public int combiners_fail;


    public Controller(int number_of_mappers, int number_of_combiners, int mappers_fail, int combiners_fail) {
        this.available_chunks = txtManager.get_chunks_name();
        this.number_of_mappers = number_of_mappers;
        this.number_of_combiners = number_of_combiners;
        this.mappers_fail = mappers_fail;
        this.combiners_fail = combiners_fail;
    }

    @Override
    public void run() {
        try {
            init_mappers(number_of_mappers, mappers_fail);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    sleep(4000); // Wait 4 seconds
                    Mapper restart_mapper = mappers.get(mappers_fail);
                    Thread restart_mapper_thread = new Thread(restart_mapper);
                    mappers_threads.set(mappers_fail, restart_mapper_thread);
                    restart_mapper_thread.start();
                    System.out.println("Mapper " + mappers_fail + " Restarted Successfully!!!!");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
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
                init_combiners(number_of_combiners, combiners_fail);

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sleep(4000); // Wait 5 seconds
                            Combiner restart_combiner = combiners.get(combiners_fail);
                            Thread restart_combiner_thread = new Thread(restart_combiner);
                            combiners_threads.set(combiners_fail, restart_combiner_thread);
                            restart_combiner_thread.start();
                            System.out.println("Combiner " + combiners_fail + " Restarted Successfully!!!!");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).run();

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

                ArrayList<String> combiner_name = txtManager.get_combiner_name();
                //to each name add the path
                String path  = "resources/Combiners_Output/";
                for (int i = 0; i < combiner_name.size(); i++) {
                    combiner_name.set(i, path + combiner_name.get(i));
                }
                Reducer_Finisher reducerFinisher = new Reducer_Finisher(combiner_name, "resources/Final_Output/word_count.txt");
                Thread reducerFinisherThread = new Thread(reducerFinisher);
                reducerFinisherThread.start();
                break;
            }
        }

    }

    void init_mappers(int number_of_mappers, int mappers_fail) throws InterruptedException {
        // init four mappers
        for (int i = 0; i < number_of_mappers; i++) {
            Mapper mapper = new Mapper(i, this.available_chunks.remove(), false);
            this.mappers.add(mapper);
            Thread mapperThread = new Thread(mapper);
            this.mappers_threads.add(mapperThread);
            System.out.println("Mapper " + i + " started");
            mapperThread.start();
        }
        if (mappers_fail  >= 0 && mappers_threads.get(mappers_fail).isAlive()) {
            mappers_threads.get(mappers_fail).stop();
            System.out.println("Mapper " + mappers_fail + " FAILED, waiting for restart...");
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

    void init_combiners(int number_of_combiners, int combiners_fail) {
        available_mapped_chunks = txtManager.get_mapper_name();
        for (int i = 0; i < number_of_combiners; i++) {
            Combiner combiner = new Combiner(i, this.available_mapped_chunks.remove());
            this.combiners.add(combiner);
            Thread combinerThread = new Thread(combiner);
            this.combiners_threads.add(combinerThread);
            System.out.println("Combiner " + i + " started");
            combinerThread.start();
        }
        if (combiners_fail  >= 0 && combiners_threads.get(combiners_fail).isAlive()) {
            combiners_threads.get(combiners_fail).stop();
            System.out.println("Combiner " + combiners_fail + " FAILED, waiting for restart...");
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

    void ask_to_stop() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Do you want to stop a Thread? (1,2,3,4,5,6,7,8 or 0 to continue)");
        int option = scanner.nextInt();
        if (option == 0) {
            return;
        } else {
            mappers_threads.get(option - 1).stop();
        }
    }

}