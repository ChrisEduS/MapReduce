public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");


        String[] results = new String[5];

        // Create and start threads
        MyThread t1 = new MyThread(3, results);
        MyThread t2 = new MyThread(1, results);
        MyThread t3 = new MyThread(5, results);
        MyThread t4 = new MyThread(2, results);
        MyThread t5 = new MyThread(4, results);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        // Wait for all threads to finish
        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
            t5.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Print the ordered results
        for (String result : results) {
            System.out.println(result);
            System.out.println();
        }


        // leer el archivo txt en resources, imprimir en pantalla
        String rutaArchivo = "resources/Texto_Original/Paulo Coelho - El Alquimista.txt";
        TxtReader txtReader = new TxtReader(rutaArchivo);
        txtReader.leerArchivo();






    }
}