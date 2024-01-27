import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        //Dividir el archivo en chunks de 32mb
        String rutaTextoAgrandado = "resources/Texto_Original/Texto_Agrandado/texto_agrandado.txt";
        int numeroChunks = 32 *1000 * 1000;
        TxtManager txtManager = new TxtManager();
        txtManager.split_file(rutaTextoAgrandado, numeroChunks);

        //Init Controller
        // Controller controller = new Controller();
        // controller.run();





    }
}