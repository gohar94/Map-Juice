import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJURLAccessPercentageReducer {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static void main(String[] args) {
    	logger.info("MJURLAccessPercentageReducer task start");
    	String inputFile = args[0];
    	System.out.println(inputFile);
    	//job1_intermedia
        String outputFilePrefix = args[1];
        System.out.println(outputFilePrefix);
        String timeStamp = args[2];
        System.out.println(timeStamp);
        String inputFileFolder = args[3];
        System.out.println(inputFileFolder);
        String seqNum = args[4];
        System.out.println(seqNum);
        //TODO spawn 10 thread
	        //according to key, cat files 
        	//cat foldername/foldername+.... -> key.txt;
        //syncronize write
        MJURLAccessPercentageReducerThread mjrt = new MJURLAccessPercentageReducerThread(inputFileFolder,inputFile,outputFilePrefix+"_"+timeStamp,seqNum,0);
        mjrt.start();
        try {
            mjrt.join();
        }catch(Exception e){
        	logger.severe(e.toString());
        }
    }
}

 class MJURLAccessPercentageReducerThread extends Thread {
    	String inputFile;
        String inputFileFolder;
        String outputFilePrefix; // including timestamp
        int id;
        String seqNum;
        private Object lock = new Object();
        
        
        public MJURLAccessPercentageReducerThread(String _inputFileFolder, String _inputFile, String _outputFilePrefix, String _seqNum,int _id) {
            inputFile = _inputFile;
            inputFileFolder = _inputFileFolder;
            outputFilePrefix = _outputFilePrefix;
            id = _id;

        }

        public void run() {
            System.out.println("Running thread "+Integer.toString(id));
            try {
            	//move this part to worker
            	//SDFSGet get = new SDFSGet(Daemon.master_list,inputFile,"sdfsFile/"+inputFile);
            	String[] folders = inputFileFolder.split("//");
            	//get.start();
            	//get.join();
            	//logger.info("Get file sucess "+inputFile);*/
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                String key = "";
                HashMap<String, Integer> map = new HashMap<String, Integer>();
                int total = 0;
                while ((key = reader.readLine()) != null) {
                   //do cat
                	String line = key;
                    key = line.split(" ")[0];
                    int val = Integer.parseInt(line.split(" ")[1]);
                    total += val;
            		map.put(key,val);
                }
                write(map, outputFilePrefix,id, total);
                System.out.println("Out");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public static synchronized void write(HashMap<String, Integer> map, String outputFilePrefix, int id, int total) {
            try {
                System.out.println("Thread writing is " + id);
                Iterator it = map.entrySet().iterator();
                File fileOut = new File(outputFilePrefix+".out");
                if (!fileOut.exists()) {
                    fileOut.createNewFile();
                }
                FileWriter fw = new FileWriter(fileOut.getAbsoluteFile(), true);      
                BufferedWriter bw = new BufferedWriter(fw);
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    String correctKey = (String)pair.getKey();
                    correctKey = correctKey.replaceAll("\\W+", "");
                    bw.write(correctKey+" : "+Integer.toString(map.get(correctKey)/total*100));
                    bw.write("\n");
                }
                    bw.flush();
                    bw.close();
                    it.remove();
                    fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
