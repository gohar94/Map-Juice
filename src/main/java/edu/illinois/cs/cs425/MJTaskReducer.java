package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJTaskReducer extends MJTask {
	int seqNum;
	String inputFilesPrefix;
	String outputFilePath;
	ArrayList<String> keys;
	ArrayList<String> inputFiles;

	public MJTaskReducer(int _seqNum,String _executablePath, String _inputFilesPrefix, String _outputFilePath,ArrayList<String> _keys, ArrayList<String> _inputFiles) {
		super(_executablePath);
		seqNum = _seqNum;
		inputFilesPrefix = _inputFilesPrefix;
		outputFilePath = _outputFilePath;
		keys = _keys;
		inputFiles  = _inputFiles;
        String temp = org.apache.commons.lang.StringUtils.join(inputFiles, ";");
        id = id + "//" + inputFilesPrefix + "//" + outputFilePath + "//" + Integer.toString(seqNum) + "//"+temp+"//"+"Reduce";
	}

    public void assign(String worker) {
    	//super.assign(worker);
        // get worker IP and port
        super.assign(worker);
        String[] parts = worker.split("//");
        String ip = parts[0];
        int portNumber = Integer.parseInt(parts[4]); 
        logger.info("Worker being assigned is " + worker);
        logger.info("Task being assigned is " + id);

        // Send message of mapTask exec input output
		Socket socket = null;
		DataOutputStream output = null;

		try {
			socket = new Socket(ip, portNumber);
			output = new DataOutputStream(socket.getOutputStream());
			output.writeUTF("reduceTask " + executablePath + " " + inputFilesPrefix + " " + outputFilePath + " " + id + " " + Daemon.selfID); // TODO do not hardcode output file here
		} catch(Exception e) {
			logger.severe(e.toString());
		} finally {
			try {
				if (socket != null)
				    socket.close();
				if (output != null)
				    output.close();
			} catch(Exception e) {
				logger.severe(e.toString());
			}
		}

        // Put in the back of queue
    }

    public void print() {
        super.print();
        System.out.println("Type is Reduce");
        System.out.println("Input Files Prefix is " + inputFilesPrefix);
        System.out.println("Output File Path is " + outputFilePath);
    }
}
