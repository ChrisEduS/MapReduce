import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

public class Controller implements Runnable{
    public int numberOfChunks;
    public ArrayList<ArrayList<String>> chunksAssigned = new ArrayList<>();
    public ArrayList<Mapper> mappers = new ArrayList<>();
    public ArrayList<Thread> mappersReady = new ArrayList<>();

    public int errorInMapper = 0;



    @Override
    public void run() {
        createMappers();
    }

    public void createMappers(){
        //read the chunks and create a mapper for each one
        numberOfChunks = TxtManager.numberOfChunks;
        System.out.println("Numero de chunks: " + numberOfChunks);

        //there will be 4 mappers in total (ideal)
        int numberOfMappers = 4;
        int chunksPerMapper = numberOfChunks / numberOfMappers;
        int chunksLeft = numberOfChunks % numberOfMappers -1;
        System.out.println("Chunks por mapper: " + chunksPerMapper);
        System.out.println("Chunks restantes que se asignan al 4to: " + chunksLeft);
        ArrayList<String>chunks = new ArrayList<>();
        chunks = pathOfChunks();

        //create the mappers
        for(int i = 0; i < numberOfMappers; i++){
            ArrayList<String> chunksForMapper = new ArrayList<>();
            for(int j = 0; j < chunksPerMapper; j++){
                chunksForMapper.add(chunks.get(0));
                chunks.remove(0);
            }
            if(chunksLeft > 0){
                chunksForMapper.add(chunks.get(0));
                chunks.remove(0);
                chunksLeft--;
            }

            chunksAssigned.add(chunksForMapper);
            Mapper mapper = new Mapper(chunksForMapper);
            mappers.add(mapper);
            Thread thread = new Thread(mapper);
            mappersReady.add(thread);
        }

        //Number of mappers created
        System.out.println("Mappers creados: " + mappersReady.size());

        //Start the mappers
        for(Thread mapper : mappersReady){
            mapper.start();
        }

        //Error in mapper
        errorInMapper();




    }

    public ArrayList<String> pathOfChunks(){
        ArrayList<String> chunks = new ArrayList<>();
        for(int i = 1; i < numberOfChunks; i++){
            String chunk_path = "resources/Chunks/_chunk" + i + ".txt";
            chunks.add(chunk_path);
        }
        return chunks;
    }

    public void errorInMapper(){
        //The user will be able to select a mapper, stop it and reassign its chunks to another mapper
        Scanner scanner = new Scanner(System.in);
        System.out.println("Ingrese el numero del mapper que desea parar (1,2,3,4) o 0 para no hacer nada: ");
        errorInMapper = scanner.nextInt();
        if (errorInMapper == 0){
            System.out.println("No se detuvo ningun mapper...");
            return;
        }
        System.out.println("Mapper parado: " + errorInMapper);
        mappersReady.get(errorInMapper-1).stop();
        restartMapper();
    }

    public void restartMapper(){
        //The user will be able to select if the mapper will be restarted or not
        Scanner scanner = new Scanner(System.in);
        System.out.println("Desea reiniciar el mapper?(En caso de que no se reinicie, se reasignaran los chunks a otro mapper) Y/N");
        String answer = scanner.nextLine();
        if(answer.equals("Y") || answer.equals("y")){
            //restart the mapper
            Mapper mapper = new Mapper(chunksAssigned.get(errorInMapper-1));
            Thread thread = new Thread(mapper);
            thread.start();
            System.out.println("Mapper reiniciado");
        }
        else{
            //reassign the chunks to another mapper
            reassignChunks();
        }
    }

    public void reassignChunks() {
        ArrayList<String> chunksToReassign = chunksAssigned.get(errorInMapper - 1);
        System.out.println("Chunks a reasignar: " + chunksToReassign);

        // Delete the mapper that was stopped
        mappers.remove(errorInMapper - 1);

        // Reassign the chunks to other mappers (3 mappers in total)
        int chunksPerMapper = chunksToReassign.size() / 3;
        for(int i = 0; i < mappers.size(); i++){
            for(int j = 0; j < chunksPerMapper; j++){
                mappers.get(i).addChunk(chunksToReassign.get(0));
                chunksToReassign.remove(0);
            }
        }
        System.out.println("Chunks reasignados correctamente");
    }

}
