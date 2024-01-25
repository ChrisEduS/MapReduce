import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {

        //Dividir el archivo en chunks de 32mb
        String rutaTextoAgrandado = "resources/Texto_Original/Texto_Agrandado/texto_agrandado.txt";
        String rutaGuardadoChunks = "resources/Chunks/";
        int numeroChunks = 32 *1000 * 1000;
        TxtReader txtReader = new TxtReader();
        txtReader.splitFile(rutaTextoAgrandado, rutaGuardadoChunks, numeroChunks);












    }
}