
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJWordCount {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static ArrayList<String> keyList = new ArrayList<String>();
    
    public static void main(String[] args) {
        logger.info("MJWordCount task start");
        String inputFilePrefix = args[0];
        String outputFilePrefix = args[1];
        String workerName = args[2];
        String timeStamp = args[3];
        try{
            logger.info("making out put folder");
            
            Runtime runtime = Runtime.getRuntime();
            String countCommand = "mkdir "+ outputFilePrefix+"_"+timeStamp;
            //output folder e.g job1_intermedia_workerName
            String[] commands = {"/bin/sh", "-c",countCommand };
            Process process = runtime.exec(commands);
            int exitCode = process.waitFor();
        }catch (Exception e){
            logger.severe("Make output folder failed");
            logger.severe(e.toString());
        }
        logger.info("making output folder sucess");
        ArrayList<MJWordCountThread> threads = new ArrayList<MJWordCountThread>();
        logger.info("Spawning 10 thread to count word");
        for (int i = 0; i < 10; i++) {
            String inputFile = inputFilePrefix +"_"+Integer.toString(i) + ".in";
            MJWordCountThread mjt = new MJWordCountThread(inputFile, outputFilePrefix+"_"+timeStamp, i);
            mjt.start();
            threads.add(mjt);
        }

        try {
            for (MJWordCountThread thread : threads) {
                thread.join();
            }
            logger.info("Start to write to keyList file");
            File file = new File(outputFilePrefix+"_"+timeStamp+"_keyList.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
            BufferedWriter bw = new BufferedWriter(fw);
            for(String key: keyList) {
                bw.write(key+"\n");
            }
            bw.flush();
            bw.close();
            fw.close();
            //compress folder here; outputFilePrefix+"_"+workerName
            try{
                logger.info("compressing file");
                Runtime runtime2 = Runtime.getRuntime();
                //String countCommand2 = "tar -zcvf "+ outputFilePrefix+"_"+workerName+".tar.gz "+outputFilePrefix+"_"+workerName;
                    //output folder e.g job1_intermedia_workerName
                String countCommand2 = "tar -cf "+outputFilePrefix+"_"+timeStamp+".tar.gz "+outputFilePrefix+"_"+timeStamp;
                logger.info("commond executed "+countCommand2);
                String[] commands2 = {"/bin/sh", "-c",countCommand2 };
                Process process2 = runtime2.exec(commands2);
                int exitCode = process2.waitFor();
            }catch(Exception e){
                e.printStackTrace();
            }
            
            logger.info("MJWordCount complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MJWordCountThread extends Thread {
    String inputFile;
    String outputFilePrefix;
    int id;
    private Object lock = new Object();
    
    public MJWordCountThread(String _inputFile, String _outputFilesPrefix, int _id) {
        inputFile = _inputFile;
        outputFilePrefix = _outputFilesPrefix;
        id = _id;
    }

    public void run() {
        System.out.println("Running");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line = "";
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            
            while ((line = reader.readLine()) != null) {
               StringTokenizer st = new StringTokenizer(line," ");
                 while (st.hasMoreTokens()) {
                     String token = st.nextToken();
                     if (map.containsKey(token)) {
                         int val = map.get(token)+1;
                         map.remove(token);
                         map.put(token, val);
                     } else {
                        map.put(token, 1);
                     }
                 }
            }
            write(map, outputFilePrefix, id);
            System.out.println("Out");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized void write(HashMap<String, Integer> map, String outputFilePrefix, int id) {
        try {
            System.out.println("Thread writing is " + id);
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                String correctKey = (String)pair.getKey();
                correctKey = correctKey.replaceAll("\\W+", "");
                String fileNameOutput = outputFilePrefix + "_" + correctKey;
                fileNameOutput = outputFilePrefix+"/"+fileNameOutput + ".out";
                File file = new File(fileNameOutput);
                if (!file.exists()) {
                    file.createNewFile();
                    MJWordCount.keyList.add(correctKey);
                    System.out.println("add a new key");
                }
                FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
                BufferedWriter bw = new BufferedWriter(fw);
                for (int i = 0; i < (Integer)pair.getValue(); i++) {
                    bw.write("1");
                    bw.write("\n");
                }
                
                bw.flush();
                bw.close();
                it.remove();
                fw.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

