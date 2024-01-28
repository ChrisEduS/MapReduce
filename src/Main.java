import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        //Dividir el archivo en chunks de 32mb
        String rutaTextoAgrandado = "texto_agrandado.txt";
        int numeroChunks = 16 *1024 * 1024;
        TxtManager txtManager = new TxtManager();
        txtManager.splitFile(rutaTextoAgrandado, numeroChunks);
        txtManager.empty_file(txtManager.getMapper_output_path()+"mapped_chunk_names.txt");
        txtManager.empty_file(txtManager.getCombiner_output_path()+"combined_chunk_names.txt");



        //Init Controller
        Controller controller = new Controller();
        Thread controllerThread = new Thread(controller);
        controllerThread.start();








    }
}