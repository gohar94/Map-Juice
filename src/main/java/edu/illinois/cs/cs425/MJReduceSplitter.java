package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.FilenameUtils;


/**
 * This thread is for splitting the Map job into smaller tasks.
 */
public class MJReduceSplitter extends Thread {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    String executablePath;
    int numReducers;
    String intermediateFilePrefix;
    String outputPath;

    public MJReduceSplitter(String _executablePath, int _numReducers, String _intermediateFilePrefix, String _outputPath) {
    	finished = false;
        executablePath = _executablePath;
    	numReducers = _numReducers;
    	intermediateFilePrefix = _intermediateFilePrefix;
    	outputPath = _outputPath;
    }

    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }
    public void printKeySet(TreeSet<String> keySet){
        System.out.println("[Debug] Show key set");
        System.out.println("------------------------------");
        for(String key:keySet){
            System.out.println(key);
        }
        System.out.println("-------------------------------");
    }
    public void printList(ArrayList<String> list){
        System.out.println("[Debug] Show key list");
        System.out.println("------------------------------");
        for(String key:list){
            System.out.println(key);
        }
        System.out.println("-------------------------------"); 
    }
    public void printTask(MJJob job){
        System.out.println("[Debug] Show MJ tasks");
        System.out.println("===============================");
        int i = 0;
        ArrayList<MJTaskReducer> tasks = job.tasks2;
        for(MJTaskReducer task:tasks){
            System.out.println("task "+Integer.toString(i));
            printList(task.keys);
            i++;
        }
        System.out.println("==============================="); 
    }
    /**
     * ThisFunction is to go through all keylist file generate by workder, make a overall key list 
     **/
    public TreeSet<String> getKeys(String intermediateFilePrefix) {
        SortedSet<String> keySet = null;
    	//intermediateFilePrefix is the same as in mapple argument: e.g: job1_intermediate"
        String prefix = intermediateFilePrefix.split("_")[0]+"_"+intermediateFilePrefix.split("_")[1];
        //prefix formate: job1_intermediate
        //go through file list, get all intermediate file
        try {
            keySet = new TreeSet<String>();
            for(String fileName:Daemon.files){
                try{
                    String fileNameP1 = fileName.split("//")[0];
                    logger.info(fileNameP1);
                    String fileNameP2 = fileNameP1.split("/")[1];
                    logger.severe(fileNameP2);
                    String[] fileNameP3 = fileNameP2.split("_");
                    if(fileNameP3.length<4){
                        logger.info("not a keyList file ");
                        continue;
                    }else{
                        String filePrefixPattern = fileNameP3[0]+"_"+fileNameP3[1];
                        logger.info("filePrefixPattern="+filePrefixPattern);
                        logger.info("prefix="+prefix);
                        if(prefix.equals(filePrefixPattern)){
                            if(fileNameP3[3].equals("keyList.txt")){
                                logger.info("Start to get a file");
                                SDFSGet getKeyFile = new SDFSGet(Daemon.master_list, fileNameP2, "sdfsFile/"+fileNameP2);
                                getKeyFile.start();
                                getKeyFile.join();
                                logger.info("get file success");
                                FileInputStream fstream = new FileInputStream(fileNameP2);
                                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                                String key = null;
                                while((key = br.readLine())!=null){
                                    keySet.add(key);
                                }
                            }else{
                                logger.info("not a keyList file ");
                                continue;
                            }                    
                        }
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        } catch (Exception e) {
            logger.severe(e.toString());
        }
    	return (TreeSet<String>)keySet;
    }
    /*
     *Here is the partition part. According to the number of reducer, partition keys in sorted key list to several task
    */
    public ArrayList<MJTaskReducer> rangePartition(TreeSet<String> keys,String executablePath, int numReducers,String intermediateFilePrefix,String outputPath) {
        ArrayList<MJTaskReducer> tasks = new ArrayList<MJTaskReducer>();
        ArrayList<String> inputFiles = new ArrayList<String>();
        //make file list
        ArrayList<String> fileHistory = new ArrayList<String>();
        for(String fileGZ:Daemon.files){
            String fileGZ1 = fileGZ.split("//")[0];
            String fileGZ2 = fileGZ1.split("/")[1];
            logger.info(fileGZ2); 
            if(FilenameUtils.getExtension(fileGZ2).equals("gz")){
               String[] fileGZ3 = fileGZ2.split("_");
               String prefix  = fileGZ3[0]+"_"+fileGZ3[1];
               logger.info(prefix);
               if(prefix.equals(intermediateFilePrefix)&&!inputFiles.contains(fileGZ2)){
                    inputFiles.add(fileGZ2);
               }else{
                    continue;
               } 
            }else{
                continue;
            }
        }
        int keyCount = keys.size();
        int eachCount = keyCount/numReducers;
        int rest = keyCount%numReducers;   //keep the modified number
        int begin = 0;
        Iterator<String> it = keys.iterator();
        int numberReducer = 0;
        while(begin<keys.size()){
            ArrayList<String> keySub = new ArrayList<String>();
            for(int i=begin;(i<begin+eachCount)&&(i<keys.size());i++){
                keySub.add(it.next());
            }
            //if keycount is not evenly divided by numReducers, add one more key to some task
            if(rest>0){
                keySub.add(it.next());
                rest--;
                begin = begin+eachCount+1;
            }else{
                begin = begin+eachCount;
            }
            logger.info("End up adding keys in a single task here");
            logger.info("write to seperate file job1_intermedia_"+Integer.toString(numberReducer)+"_subNo_keyList.txt");
            Iterator<String> itWrite= keySub.iterator();
            int eachSub = keySub.size()/10;
            if(keySub.size()%10!=0){
                eachSub++;
            } 
            for(int i = 0;i<10;i++){
                try{
                    logger.info("Start writing file "+intermediateFilePrefix+"_"+Integer.toString(numberReducer)+"_"+Integer.toString(i)+"_keyList.txt");
                    String subKeyListFile = intermediateFilePrefix+"_"+Integer.toString(numberReducer)+"_"+Integer.toString(i)+"_keyList.txt";
                    File file = new File(subKeyListFile);
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
                    BufferedWriter bw = new BufferedWriter(fw);
                    int j = 0;
                    while(j<eachSub&&itWrite.hasNext()) {
                        bw.write(itWrite.next()+"\n");
                        j++;
                    }
                    bw.flush();
                    bw.close();
                    fw.close();
                    SDFSPut putKeyList =new SDFSPut(Daemon.master_list,subKeyListFile,"sdfsFile/"+subKeyListFile); // put the files in SDFS
                    putKeyList.start();
                    putKeyList.join();
                }catch(Exception e){
                    logger.severe(e.toString());
                }
                
            }    
            //have to include number in the tasks;
            MJTaskReducer newTask = new MJTaskReducer(numberReducer,executablePath,intermediateFilePrefix,outputPath,keySub,inputFiles);
            tasks.add(newTask);
            numberReducer++;
            logger.info("add new task");
        }
        return tasks;
    }


    public MJJob makeJob(TreeSet keySet,String executablePath, int numReducers,String intermediateFilePrefix,String outputPath) {
        // put the list in the Job object and return the job object
        ArrayList<MJTaskReducer> tasks = new ArrayList<MJTaskReducer>();
        tasks = rangePartition(keySet,executablePath,numReducers,intermediateFilePrefix,outputPath);

        MJJob job = new MJJob("Reduce",executablePath, new ArrayList<MJTaskMapper>(), tasks);
        return job;
    }

    public void run() {
		try {
            //make a entire list of keys
			TreeSet<String> keySet = getKeys(intermediateFilePrefix);
            //shard keys into chunks. put key sublist into several different task and wrap up a job
            MJJob reduceJob = makeJob(keySet,executablePath,numReducers,intermediateFilePrefix,outputPath);     
            //printTask(reduceJob);
            Daemon.jobs.offer(reduceJob);
		} catch (Exception e) {
			logger.severe(e.toString());
		}
    }
}
