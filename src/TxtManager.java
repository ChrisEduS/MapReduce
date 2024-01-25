import java.io.*;
import java.util.ArrayList;

public class TxtManager {

    public static int numberOfChunks = 0;



    // Método para leer el archivo de texto
    public ArrayList<String> leerArchivo(String rutaArchivo) {
        try {
            File archivo = new File(rutaArchivo);
            FileReader fr = new FileReader(archivo);
            BufferedReader br = new BufferedReader(fr);

            String linea;
            ArrayList<String> words = new ArrayList<String>();

            while ((linea = br.readLine()) != null) {
                //split the line into words and save to an ArrayList
                String[] lineWords = linea.split(" ");
                for (String word : lineWords) {
                    words.add(word);
                }
            }



            br.close();
            fr.close();

            return words;

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
           return null;
    }


    // split a document into 32mb chunks
    public static void splitFile(String inputFilePath, String outputPrefix, int chunkSize) {
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFilePath))) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkNumber = 1;

            while ((bytesRead = bis.read(buffer, 0, chunkSize)) > 0) {
                String outputFilePath = outputPrefix + "_chunk" + chunkNumber + ".txt";
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFilePath))) {
                    bos.write(buffer, 0, bytesRead);
                }
                chunkNumber++;
                buffer = new byte[chunkSize];
            }
            numberOfChunks = chunkNumber;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

