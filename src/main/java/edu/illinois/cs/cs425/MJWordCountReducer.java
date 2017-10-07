import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJWordCountReducer {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static void main(String[] args) {
    	logger.info("MJWordCountReducer task start");
    	String inputFilePrefix = args[0];
    	System.out.println(inputFilePrefix);
    	//job1_intermedia
        String outputFilePrefix = args[1];
        System.out.println(outputFilePrefix);
        String timeStamp = args[2];
        System.out.println(timeStamp);
        String inputFileFolder = args[3];
        System.out.println(inputFileFolder);
        String seqNum = args[4];
        System.out.println(seqNum);

        ArrayList<MJWordCountReducerThread> threads = new ArrayList<MJWordCountReducerThread>();
        logger.info("Spawning 10 thread to count word");
        for (int i = 0; i < 10; i++) {
            String inputFile = inputFilePrefix +"_"+seqNum+"_"+Integer.toString(i) + "_keyList.txt";
            MJWordCountReducerThread mjrt = new MJWordCountReducerThread(inputFileFolder,inputFile,outputFilePrefix+"_"+timeStamp,seqNum,i);
            mjrt.start();
            threads.add(mjrt);
        }
        try {
            for (MJWordCountReducerThread thread : threads) {
                thread.join();
            }
        }catch(Exception e){
        	logger.severe(e.toString());
        }
    }
}

 class MJWordCountReducerThread extends Thread {
    	String inputFile;
        String inputFileFolder;
        String outputFilePrefix; // including timestamp
        int id;
        String seqNum;
        private Object lock = new Object();
        
        
        public MJWordCountReducerThread(String _inputFileFolder, String _inputFile, String _outputFilePrefix, String _seqNum,int _id) {
            inputFile = _inputFile;
            inputFileFolder = _inputFileFolder;
            outputFilePrefix = _outputFilePrefix;
            id = _id;

        }

        public void run() {
            System.out.println("Running thread "+Integer.toString(id));
            try {
            	String[] folders = inputFileFolder.split("//");
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                String key = "";
                HashMap<String, Integer> map = new HashMap<String, Integer>();
                while ((key = reader.readLine()) != null) {
                   //do cat
                	StringBuilder catCommand  = new StringBuilder();
                	catCommand.append("cat ");
                	for(String folderBaseName:folders){
                		catCommand.append(folderBaseName+"/"+folderBaseName+"_"+key+".out ");
                	}
                	catCommand.append("> "+key+".txt");
                	Runtime runtime = Runtime.getRuntime();
 					String command = catCommand.toString();
 					System.out.println("cat command line is "+command);
 					String[] commands = {"/bin/sh", "-c",command };
            		Process process = runtime.exec(commands);
            		int exitCode = process.waitFor();
            		//read key value pair;

            		BufferedReader reader2 = new BufferedReader(new FileReader(key+".txt"));
            		String value;
            		int outValue = 0;
            		while((value = reader2.readLine())!=null){
            			outValue++;
            		}
            		map.put(key,outValue);
                }
                write(map, outputFilePrefix,id);
                System.out.println("Out");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public static synchronized void write(HashMap<String, Integer> map, String outputFilePrefix, int id) {
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
                    bw.write(correctKey+" : "+Integer.toString(map.get(correctKey)));
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
