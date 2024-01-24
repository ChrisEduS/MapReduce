import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TxtReader {
    // Atributo para almacenar la ruta del archivo
    private final String rutaArchivo;

    // Constructor que recibe la ruta del archivo
    public TxtReader(String rutaArchivo) {
        this.rutaArchivo = rutaArchivo;
    }

    // Método para leer el archivo de texto
    public void leerArchivo() {
        try {
            File archivo = new File(rutaArchivo);
            FileReader fr = new FileReader(archivo);
            BufferedReader br = new BufferedReader(fr);

            String linea;
            while ((linea = br.readLine()) != null) {
                System.out.println(linea);
            }

            br.close();
            fr.close();

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}

