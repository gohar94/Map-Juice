package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJTaskMapper extends MJTask {
	String inputFilePath;
	String outputFilesPrefix;

	public MJTaskMapper(String _executablePath, String _inputFilePath, String _outputFilesPrefix) {
		super(_executablePath);
		inputFilePath = _inputFilePath;
		outputFilesPrefix = _outputFilesPrefix;
        id = id + "//" + inputFilePath + "//" + outputFilesPrefix + "//" + "Map"; // TODO add some unique keys to id
	}

    public void assign(String worker) {
    	super.assign(worker);
        // get worker IP and port
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
			output.writeUTF("mapTask " + executablePath + " " + inputFilePath + " " + outputFilesPrefix + " " + id + " " + Daemon.selfID); // TODO do not hardcode output file here
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
        System.out.println("Type is Map");
        System.out.println("Input File Path is " + inputFilePath);
        System.out.println("Output Files Prefix is " + outputFilesPrefix);
    }
}
