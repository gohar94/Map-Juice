package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * This thread is for splitting the Map job into smaller tasks.
 */
public class MJMapSplitter extends Thread {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    String executablePath;
    int numMappers;
    String intermediateFilePrefix;
    String inputPath;

    public MJMapSplitter(String _executablePath, int _numMappers, String _intermediateFilePrefix, String _inputPath) {
    	finished = false;
        executablePath = _executablePath;
    	numMappers = _numMappers;
    	intermediateFilePrefix = _intermediateFilePrefix;
    	inputPath = _inputPath;
    }

    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }
    /*
     * This function is to get all input file and merge to one big file
     */
    public String getFile(String inputPath) {
        String cattedInputFile = "";
        
        try {
            cattedInputFile = "input.in";
            String command = "rm -rf inputs && mkdir inputs";
            String[] commandLine = {"/bin/sh","-c",command};
            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec(commandLine);
            int exitCode = process.waitFor();
            
            List<String> getFileList = new ArrayList<String>();
            for(String fileName:Daemon.files){
                //Formate fileName: sdfsFile/Job1_example.txt//babalba//bababa
                try{
                    String dir = fileName.split("//")[0].split("/")[1].split("_")[0];
                    if(dir.equals(inputPath)) {
                    //add sdfsFile/Job1_example.txt to the list
                        if(!getFileList.contains(fileName.split("//")[0])){
                            getFileList.add(fileName.split("//")[0]);     
                        }             
                    }
                }catch (Exception e){
                    logger.severe("Format does not match "+ fileName);
                    continue;
                }
                
            }
            
            ArrayList<SDFSGet> threads = new ArrayList<SDFSGet>();
            for(String getTheFile: getFileList){
                //getTheFile formate: sdfsFile/Job1_example.txt
                String getTheFileName = getTheFile.split("/")[1];
                SDFSGet get = new SDFSGet(Daemon.master_list, "inputs/"+getTheFileName, getTheFile);
                get.start();
                threads.add(get);
            }

            for (SDFSGet thread : threads) {
                thread.join();
            }
            command = "cat inputs/* > " + cattedInputFile;
            String[] commandLine2 = {"/bin/sh","-c",command};
            Process process2 = runtime.exec(commandLine2);
            exitCode = process2.waitFor();
        } catch (Exception e) {
            logger.severe(e.toString());
        }

    	return cattedInputFile;
    }
    /**
    * This function is to shard input files into small chunks and wrap up as maple task
    */
    public MJJob makeJob(String cattedInputFile,int numMappers,String intermediateFilePrefix,String inputPath) {
        ArrayList<MJTaskMapper> tasks = new ArrayList<MJTaskMapper>();
        try{
            long inputFileLines = 0;
            String lineString = null;
            Runtime runtime = Runtime.getRuntime();
            String countCommand = "wc -l input.in";
            String[] commands = {"/bin/sh", "-c",countCommand };
            Process process = runtime.exec(commands);
            int exitCode = process.waitFor();
            BufferedReader processInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            if((lineString=processInput.readLine())==null){
                inputFileLines = 0;
            }else{
                logger.info(lineString);
                // lineString = lineString.split(" ")[3];
                // inputFileLines = Long.parseLong(lineString);
                String pattern = "\\S*(\\d+).*";
                Pattern r = Pattern.compile(pattern);
                Matcher m = r.matcher(lineString);
                if (m.find()) {
                    String num = m.group(0);
                    System.out.println(num);
                    num = num.split(" ")[0];
                    inputFileLines = Long.parseLong(num);
                }
            }
            logger.info("total lines in input file: "+Long.toString(inputFileLines));
            // split file by size
            //File f = new File("input.in");
            // divide the lines, and make # numMappers files (serially for now)
            //long size = f.length();
            FileInputStream fstream = new FileInputStream("input.in"); 
            DataInputStream in = new DataInputStream(fstream);
            long number = (long)numMappers;
            //int eachSize = (int)(size/number);
            //split file to 10*number chunks;
            long eachLine = inputFileLines/(number*10);
            long rest = inputFileLines%(number*10);
            if(rest!=0){
                    eachLine++;
            }
            logger.info("Split file to "+Long.toString(eachLine) +" each");
            BufferedReader br = new BufferedReader(new InputStreamReader(in)); 
            String strLine;
            ArrayList<Thread> threads = new ArrayList<Thread>();
            int countToNumber = 0;
            int countToTen = 0;
            for(int i=0;i<number*10;i++){
                if(countToTen==10){
                    countToTen = 0;
                    countToNumber++;
                }   
                String newFileName = inputPath+"_input_"+String.format("%02d", countToNumber)+"_"+Integer.toString(countToTen)+".in";
                String newFileNamePrefix = inputPath+"_input_"+String.format("%02d", countToNumber);
                FileWriter newFile = new FileWriter(newFileName);     // Destination File Location  
                BufferedWriter out = new BufferedWriter(newFile);
                long lineNo=eachLine;
                logger.info("Starting writting newfile "+newFileName);
                
                long j;
                long lineCount = 0;
                int x;
                while ((x = br.read()) != -1) {
                    if ((char)x == '\n') {
                        lineCount++;
                    }
                    out.write((char)x);
                    if(lineCount == eachLine){
                        break;
                    }
                }
                out.close();   
                SDFSPut put = new SDFSPut(Daemon.master_list,newFileName,"sdfsFile/"+newFileName); // put the files in SDFS
                Thread thread = new Thread(put);
                thread.start();
                threads.add(thread);
                if(countToTen==9){
                    MJTaskMapper newTask = new MJTaskMapper(executablePath,"sdfsFile/"+newFileNamePrefix,intermediateFilePrefix); // make a list of MJTaskMapper where the inputFile is the SDFS file
                    tasks.add(newTask);  
                    logger.info("Add new task successfully");
                }
                 
                countToTen++;  
            }
            in.close();
            for (Thread thread : threads) {
                thread.join();
            }
        } catch(Exception e){
            logger.info(e.toString());
        }
        // put the list in the Job object and return the job object       
        MJJob job = new MJJob("Map", executablePath, tasks, new ArrayList<MJTaskReducer>());
        return job;
    }
    public void run() {
		try {
			String tmpInputFile = getFile(inputPath);
			logger.info("Got the input file locally - " + tmpInputFile);
            MJJob job = makeJob(executablePath,numMappers,intermediateFilePrefix,inputPath);
            job.print();
            Daemon.jobs.offer(job);
		} catch (Exception e) {
			logger.severe(e.toString());
		}
    }
}
