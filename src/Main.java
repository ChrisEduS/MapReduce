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

        int mappers_fail = 0;
        int combiners_fail = 0;

        //fail?
        String fail = "";
        System.out.println("Do you want to fail a mapper or combiner? (y/n)");
        fail = scanner.next();
        switch (fail){
            case "y":
                System.out.println("Which one? (mapper/combiner/both)");
                String which_one = scanner.next();
                switch (which_one){
                    case "mapper":
                        int upper = number_of_mappers - 1;
                        System.out.println("Which Mapper? (0 - " + upper + ")");
                        mappers_fail = scanner.nextInt();
                        break;
                    case "combiner":
                        int upper2 = number_of_combiners - 1;
                        System.out.println("Which Combiner? (0 - " + upper2 + ")");
                        combiners_fail = scanner.nextInt();
                        break;
                    case "both":
                        int upper3 = number_of_mappers - 1;
                        int upper4 = number_of_combiners - 1;
                        System.out.println("Which Mapper? (0 - " + upper3 + ")");
                        mappers_fail = scanner.nextInt();
                        System.out.println("Which Combiner? (0 - " + upper4 + ")");
                        combiners_fail = scanner.nextInt();
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
        Controller controller = new Controller(number_of_mappers, number_of_combiners, mappers_fail, combiners_fail);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();








    }
}