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

    public boolean fail_mappers;
    public boolean fail_combiners;

    public int id_mapper_fail;
    public int id_combiner_fail;


    public Controller(int number_of_mappers, int number_of_combiners, boolean fail_mappers, boolean fail_combiners, int id_mapper_fail, int id_combiner_fail) {
        this.available_chunks = txtManager.get_chunks_name();
        this.number_of_mappers = number_of_mappers;
        this.number_of_combiners = number_of_combiners;
        this.id_mapper_fail = id_mapper_fail;
        this.id_combiner_fail = id_combiner_fail;
        this.fail_mappers = fail_mappers;
        this.fail_combiners = fail_combiners;
    }

    @Override
    public void run() {
        try {
            init_mappers(number_of_mappers, id_mapper_fail);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (this.fail_mappers){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        sleep(4000); 
                        Mapper restart_mapper = mappers.get(id_mapper_fail);
                        Thread restart_mapper_thread = new Thread(restart_mapper);
                        mappers_threads.set(id_mapper_fail, restart_mapper_thread);
                        restart_mapper_thread.start();
                        System.out.println("Mapper " + id_mapper_fail + " Restarted Successfully!!!!");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
        }
        
        while (true) {
            if (!available_chunks.isEmpty()) {
                verify_state_mappers();
                }
            if (all_mappers_finished()){
                System.out.println("Map finished");
                break;
            }
        }

        try {
            init_combiners(number_of_combiners, id_combiner_fail);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        
        if (this.fail_combiners){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        sleep(8000); 
                        Combiner restart_combiner = combiners.get(id_combiner_fail);
                        Thread restart_combiner_thread = new Thread(restart_combiner);
                        combiners_threads.set(id_combiner_fail, restart_combiner_thread);
                        restart_combiner_thread.start();
                        System.out.println("Combiner " + id_combiner_fail + " Restarted Successfully!!!!");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
        }
        
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
            
        }


    void init_mappers(int number_of_mappers, int id_mapper_fail) throws InterruptedException {
        // init four mappers
        for (int i = 0; i < number_of_mappers; i++) {
            Mapper mapper = new Mapper(i, this.available_chunks.remove(), false);
            this.mappers.add(mapper);
            Thread mapperThread = new Thread(mapper);
            this.mappers_threads.add(mapperThread);
            System.out.println("Mapper " + i + " started");
            mapperThread.start();
        }
        if (this.fail_mappers && mappers_threads.get(id_mapper_fail).isAlive()) {
            mappers_threads.get(id_mapper_fail).stop();
            System.out.println("Mapper " + id_mapper_fail + " FAILED, waiting for restart...");
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

    void init_combiners(int number_of_combiners, int id_combiner_fail) throws InterruptedException {
        available_mapped_chunks = txtManager.get_mapper_name();
        for (int i = 0; i < number_of_combiners; i++) {
            Combiner combiner = new Combiner(i, this.available_mapped_chunks.remove());
            this.combiners.add(combiner);
            Thread combinerThread = new Thread(combiner);
            this.combiners_threads.add(combinerThread);
            System.out.println("Combiner " + i + " started");
            combinerThread.start();
        }
        if (this.fail_combiners && combiners_threads.get(id_combiner_fail).isAlive()) {
            combiners_threads.get(id_combiner_fail).stop();
            System.out.println("Combiner " + id_combiner_fail + " FAILED, waiting for restart...");
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