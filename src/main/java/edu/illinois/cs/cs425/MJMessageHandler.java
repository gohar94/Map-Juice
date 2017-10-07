package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FilenameUtils;
import java.text.SimpleDateFormat;



/**
 * This thread is for handling MJ commands sent to the master.
 */
public class MJMessageHandler extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    Socket socket;

    public MJMessageHandler(Socket _socket) {
        finished = false;
        socket = _socket;
    }

    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }

    public void completed(String taskId, String masterID) {
        try {
            String completedReply = "completed " + taskId + " " + Daemon.selfID;
            logger.info("Sending completed message = " + completedReply);

            String[] parts = masterID.split("//");
            String ip = parts[0];
            int portNumber = Integer.parseInt(parts[4]);
            Socket socketMasterMJ = new Socket(ip, portNumber);
            DataOutputStream outputMJ = new DataOutputStream(socketMasterMJ.getOutputStream());
            outputMJ.writeUTF(completedReply);
            outputMJ.close();
            socketMasterMJ.close();
        } catch (Exception e) {
            logger.severe(e.toString());
        }
    }

    /**
     * This function parses the message and handles it appropriately. 
     */
    public void run() {
        DataInputStream input = null;
        DataOutputStream output = null;
        
        try {
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            
            String message  = input.readUTF();
            logger.info("Incoming message is " + message);

            String command = message.split(" ")[0];
            String[] arguments = Arrays.copyOfRange(message.split(" "), 1, message.split(" ").length);

            if (command.equals("mapJob")) {
                // mapJob <executable path> <number of mappers> <intermediate filename prefix> <input file or directory> 
                
                String executablePath = arguments[0];
                int numMappers = Integer.parseInt(arguments[1]);
                String intermediatePrefix = arguments[2];
                String inputPath = arguments[3];
                
                MJMapSplitter splitter = new MJMapSplitter(executablePath, numMappers, intermediatePrefix, inputPath);
                splitter.start();
                splitter.join();
            } else if (command.equals("reduceJob")) {
                // reduceJob <executable path> <number of reducers> <intermediate filename prefix> <output filename> 
                // if message comes to master as reduceJob, start partition
                String executablePath = arguments[0];
                int numReducers = Integer.parseInt(arguments[1]);
                String intermediatePrefix = arguments[2];
                String outputPath = arguments[3];
                
                MJReduceSplitter splitter = new MJReduceSplitter(executablePath, numReducers, intermediatePrefix, outputPath);
                splitter.start();
                splitter.join();
            } else if (command.equals("mapTask")) {
                // if message comes to workder as mapTask, worker will start doing task

                String executablePath = arguments[0];
                String inputPath = arguments[1].split("/")[1];
                System.out.println("inputPath " + inputPath);
                String outputPath = arguments[2];
                String taskId = arguments[3];
                String masterId = arguments[4];
                String[] workerID = Daemon.selfID.split("//");

                logger.info("Going to get exec file");

                ArrayList<SDFSGet> threads = new ArrayList<SDFSGet>();
                SDFSGet getExec = new SDFSGet(Daemon.master_list,executablePath,"sdfsFile/"+executablePath);
                getExec.start();

                if (executablePath.equals("MJWordCount.class")) {
                    logger.info("Getting executable thread");
                    SDFSGet getExec2 = new SDFSGet(Daemon.master_list,"MJWordCountThread.class","sdfsFile/MJWordCountThread.class");
                    getExec2.start();
                    threads.add(getExec2);
                }

                threads.add(getExec);
                for (int i = 0; i < 10; i++) {
                    String getTheFile = inputPath +"_"+Integer.toString(i) + ".in";
                    SDFSGet get = new SDFSGet(Daemon.master_list, getTheFile, "sdfsFile/"+getTheFile);
                    get.start();
                    threads.add(get);
                }

                logger.info("Going to get input files");

                for (SDFSGet thread : threads) {
                    thread.join();
                }

                logger.info("Going to execute command");

                executablePath = FilenameUtils.getBaseName(executablePath);
                Date timeOfTask = new Date();
                String timeStamp = Long.toString(timeOfTask.getTime());
                String commandLine = "java "+executablePath+" "+inputPath+" "+outputPath+" "+workerID[0]+":"+workerID[4]+" "+timeStamp;
                String[] command2 = {"/bin/sh","-c",commandLine};
                Runtime runtime = Runtime.getRuntime();
                Process process = runtime.exec(command2);
                // process.waitFor(); // TODO should this be here?

                BufferedReader stdInput = new BufferedReader(new 
                     InputStreamReader(process.getInputStream()));

                BufferedReader stdError = new BufferedReader(new 
                     InputStreamReader(process.getErrorStream()));

                // read the output from the command
                // System.out.println("Here is the standard output of the command:\n");
                String s = null;
                while ((s = stdInput.readLine()) != null) {
                    System.out.println(s);
                }
                // read any errors from the attempted command
                // System.out.println("Here is the standard error of the command (if any):\n");
                while ((s = stdError.readLine()) != null) {
                    System.out.println(s);
                }

                process.waitFor(); // TODO should this be here?

                logger.info("command executed is " + commandLine);
                logger.info("Start to put files");
                String folderName = outputPath+"_"+timeStamp;
                SDFSPut put = new SDFSPut(Daemon.master_list,folderName+".tar.gz","sdfsFile/"+folderName+".tar.gz"); // put the files in SDFS
                put.start();
                put.join();
                // logger.info("Putting folder");
                SDFSPut put2 = new SDFSPut(Daemon.master_list,folderName+"_keyList.txt","sdfsFile/"+folderName+"_keyList.txt"); // put the files in SDFS
                put2.start();
                put2.join();
                // logger.info("Putting keyList file");
                    //Thread.sleep(500);
                    // threads2.add(put);
                    // Runnable putter = new SDFSPut(Daemon.master_list,fileName,"sdfsFile/"+fileName); // put the files in SDFS
                    // logger.info("Starting a put thread");
                    // executor.execute(putter);
                
                
                logger.info("Done all put threads");
                // executor.shutdown();
                // while (!executor.isTerminated()) {}
                
                completed(taskId, masterId);
            } else if (command.equals("reduceTask")) {
                // if message comes to worker as reduceTask, worker start execute reduce task
                String executablePath = arguments[0];
                String intermediatePrefix = arguments[1];
                String outputPath = arguments[2];
                String taskId = arguments[3];
                String masterId = arguments[4];
                String seqNum = taskId.split("//")[3];//Woker will get keyList base on this number
                String fileToGet = taskId.split("//")[4];
                String[] fileToGetList = fileToGet.split(";");
                StringBuilder fileFolderBaseName = new StringBuilder();
                //Do get all key list files;
                ArrayList<SDFSGet> threads = new ArrayList<SDFSGet>();
                SDFSGet getExec = new SDFSGet(Daemon.master_list,executablePath,"sdfsFile/"+executablePath);
                getExec.start();
                threads.add(getExec);
                if (executablePath.equals("MJWordCountReducer.class")) {
                    // logger.info("Getting executable thread");
                    SDFSGet getExec2 = new SDFSGet(Daemon.master_list,"MJWordCountReducerThread.class","sdfsFile/MJWordCountReducerThread.class");
                    getExec2.start();
                    threads.add(getExec2);
                }
                logger.info("Get executable file sucessfully");
                for(int i=0;i<10;i++){
                    //getTheFile formate: sdfsFile/Job1_example.txt    
                    String getKeyFileName = intermediatePrefix+"_"+seqNum+"_"+Integer.toString(i)+"_keyList.txt";
                    // logger.info("getting key list files "+getKeyFileName);
                    SDFSGet get = new SDFSGet(Daemon.master_list, getKeyFileName, "sdfsFile/"+getKeyFileName);
                    get.start();
                    threads.add(get);
                    // logger.info("get key list sucess "+getKeyFileName);
                }

                for (SDFSGet thread : threads) {
                    if(thread.interrupted()){
                        logger.severe("Get fail due to some failure, restart");
                        thread.start();
                    }
                    thread.join();
                   
                }

                // logger.info("HAHAHAH I AM HERE");
                //when it is the first time get task from this job, make a directory and get file
                File folder = new File(intermediatePrefix);
                if(!folder.isDirectory()){ 
                    String command1 = "mkdir "+intermediatePrefix;
                    String[] commandLine1 = {"/bin/sh","-c",command1};
                    Runtime runtime1 = Runtime.getRuntime();
                    Process process1 = runtime1.exec(commandLine1);
                    int exitCode = process1.waitFor();
                    // logger.info("make folder "+intermediatePrefix+ " sucess");
                    // get input data for reduce task
                    for(String fileGZ:fileToGetList){
                        // logger.info("Start getting "+fileGZ);
                        SDFSGet get = new SDFSGet(Daemon.master_list,fileGZ,"sdfsFile/"+fileGZ);
                        try{
                            get.start();
                            if(get.interrupted()){
                                logger.info("Due to some failure, restart");
                                get.start();
                            }
                            get.join();
                        }catch(Exception e){
                            logger.info("Due to some failure, restart");
                            get.start();
                        } 
                        String baseName = fileGZ.split(".tar.gz")[0];
                        fileFolderBaseName.append(baseName+"//");
                        String command2 = "tar -xf "+fileGZ;
                        String[] commandLine2 = {"/bin/sh","-c",command2};
                        Runtime runtime2 = Runtime.getRuntime();
                        Process process2 = runtime2.exec(commandLine2);
                        int exitCode2 = process2.waitFor();
                    } 
                }else{
                    for(String fileGZ:fileToGetList){
                        String baseName = fileGZ.split(".tar.gz")[0];
                        fileFolderBaseName.append(baseName+"//");
                    }
                }   
                //execute task
                executablePath = FilenameUtils.getBaseName(executablePath);
                Date timeOfTask = new Date();
                String fileFolderBaseNameString = fileFolderBaseName.toString();
                String timeStamp = Long.toString(timeOfTask.getTime());
                String commandLine3 = "java "+executablePath+" "+intermediatePrefix +" "+outputPath+" "+timeStamp+" "+fileFolderBaseNameString+" "+seqNum;
                String[] command3 = {"/bin/sh","-c",commandLine3};
                Runtime runtime3 = Runtime.getRuntime();
                // logger.info("Command to be executed "+commandLine3);
                Process process3 = runtime3.exec(command3);
                // process3.waitFor(); // TODO should this be here?

                BufferedReader stdInput = new BufferedReader(new 
                     InputStreamReader(process3.getInputStream()));

                BufferedReader stdError = new BufferedReader(new 
                     InputStreamReader(process3.getErrorStream()));

                // read the output from the command
                // System.out.println("Here is the standard output of the command:\n");
                String s = null;
                while ((s = stdInput.readLine()) != null) {
                     System.out.println(s);
                }
                // read any errors from the attempted command
                // System.out.println("Here is the standard error of the command (if any):\n");
                while ((s = stdError.readLine()) != null) {
                     System.out.println(s);
                }

                logger.info("command executed is " + commandLine3);
                //TODO put output files to sdfs
                SDFSPut put = new SDFSPut(Daemon.master_list,outputPath+"_"+timeStamp+".out","sdfsFile/"+outputPath+"_"+timeStamp+".out");
                put.start();
                put.join();
                completed(taskId, masterId);
            } else if (command.equals("completed")) {
                String taskId = arguments[0];
                String worker = arguments[1];
                if (Daemon.allTasks.containsKey(taskId)) {
                    MJTask task = Daemon.allTasks.get(taskId);
                    task.isCompleted = true;
                    MJJob job = task.job;
                    if (job == null) {
                        logger.info("No job on this task");
                    } else {
                        boolean isCompleted = true;
                        for (MJTaskMapper taskx : job.tasks) {
                            if (!taskx.isCompleted) {
                                isCompleted = false;
                                break;
                            }
                        }
                        for (MJTaskReducer taskx : job.tasks2) {
                            if (!taskx.isCompleted) {
                                isCompleted = false;
                                break;
                            }
                        }
                        logger.info("Job status completion is " + isCompleted);
                        if (isCompleted) {
                            logger.info("This job is complete");
                            // if it is a reducer job, get all output file and merge to one
                            if (job.type.equals("Reduce")) {
                                logger.info("Job type was Reduce");
                                logger.info(taskId);
                                String outputPath = taskId.split("//")[2];
                                for(String fileFinal:Daemon.files){
                                    String fileFinal1 = fileFinal.split("//")[0];
                                    logger.info(fileFinal1);
                                    String fileFinal2 = fileFinal1.split("/")[1];
                                    String[] fileFinal3 = fileFinal2.split("_");
                                    try{
                                        String finalPrefix = fileFinal3[0]+"_"+fileFinal3[1];
                                        // logger.info("Start looking for final files "+finalPrefix);
                                        if(finalPrefix.equals(outputPath)){
                                            SDFSGet finalGet = new SDFSGet(Daemon.master_list,fileFinal2,fileFinal1);
                                            finalGet.start();
                                            finalGet.join();
                                        }
                                    }catch(Exception e){
                                        continue;
                                    }           
                                }
                                logger.info("Get all files");
                                String commandLine4 = "cat "+outputPath+"*.out > temp.txt && sort temp.txt > final_"+outputPath+".txt";
                                String[] command4 = {"/bin/sh","-c",commandLine4};
                                Runtime runtime4 = Runtime.getRuntime();
                                // logger.info("Command to be executed "+commandLine4);
                                Process process4 = runtime4.exec(command4);
                                logger.info("Generate final file output.");
                            } else {
                                logger.info("Job type was Map");
                            }
                        } else {
                            logger.info("This job is not yet complete");
                        }
                    }
                }
                Daemon.workers.add(worker);
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        } finally {
            try {
                if (socket != null)
                    socket.close();
                if (input != null)
                    input.close();
                if (output != null)
                    output.close();
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
