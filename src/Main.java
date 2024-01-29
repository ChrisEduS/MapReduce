import static java.lang.Thread.sleep;

import java.util.Scanner;
public class Main {
    public static void main(String[] args) throws InterruptedException {

        //Dividir el archivo en chunks de 32mb
        String rutaTextoAgrandado = "texto_agrandado.txt";
        int numeroChunks = 16 *1024 * 1024;
        TxtManager txtManager = new TxtManager();
        txtManager.splitFile(rutaTextoAgrandado, numeroChunks);
        txtManager.empty_file(txtManager.getMapper_output_path()+"mapped_chunk_names.txt");
        txtManager.empty_file(txtManager.getCombiner_output_path()+"combined_chunk_names.txt");

       //Ask for the number of mappers and combiners
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the number of mappers: ");
        int number_of_mappers = scanner.nextInt();
        System.out.println("Enter the number of combiners: ");
        int number_of_combiners = scanner.nextInt();

        boolean fail_controller = false;
        boolean fail_mappers = false;
        boolean fail_combiners = false;

        int id_mapper_fail = 0;
        int id_combiner_fail = 0;
        

        //fail controller
        String controller_case = "";
        System.out.println("Do you want to fail the controller? (y/n)");
        controller_case = scanner.next();
        switch (controller_case){
            case "y":
                fail_controller = true;
                break;
            case "n":
                break;
        }

        //fail nodes
        String fail_nodes = "";
        System.out.println("Do you want to fail a mapper or combiner? (y/n)");
        fail_nodes = scanner.next();
        switch (fail_nodes){
            case "y":
                System.out.println("Which one? (mapper/combiner/both)");
                String which_one = scanner.next();
                int upper_mappers = number_of_mappers - 1;
                int upper_combiners = number_of_combiners - 1;
                switch (which_one){
                    case "mapper":
                        System.out.println("Which Mapper? (0 - " + upper_mappers + ")");
                        id_mapper_fail = scanner.nextInt();
                        fail_mappers = true;
                        break;
                    case "combiner":
                        System.out.println("Which Combiner? (0 - " + upper_combiners + ")");
                        id_combiner_fail = scanner.nextInt();
                        fail_combiners = true;
                        break;
                    case "both":
                        System.out.println("Which Mapper? (0 - " + upper_mappers + ")");
                        id_mapper_fail = scanner.nextInt();
                        System.out.println("Which Combiner? (0 - " + upper_combiners + ")");
                        id_combiner_fail = scanner.nextInt();
                        fail_mappers = true;
                        fail_combiners = true;
                        break;
                    default:
                        System.out.println("Invalid option");
                        break;
                }
                break;
            case "n":
                break;
        }

        //Init Controller
        Controller controller = new Controller(number_of_mappers, number_of_combiners, fail_mappers, fail_combiners, id_mapper_fail, id_combiner_fail);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();

        if (fail_controller){
            controllerThread.stop();
            System.out.println("Controller FAILED, waiting for restart...");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        sleep(3000);
                        Thread controllerThread = new Thread(controller);
                        controllerThread.start();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }
}
