package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This thread is for the master to listen to all incoming messages and take action.
*/
public class Maple {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	// Socket socket;

	public Maple() {
		// try {
			
		// } catch (Exception e) {
		// 	logger.severe(e.toString());
		// }
	}

	public void run(String executablePath, int numMaples, String intermediatePrefix, String inputDirectory) {
		try {
			Socket socket = new Socket("localhost", 6688);
			DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF("mapJob " + executablePath + " " + Integer.toString(numMaples) + " " + intermediatePrefix + " " + inputDirectory);

            String message = input.readUTF();
            logger.info("Message is " + message);
		} catch (Exception e) {
			logger.info("Could not connect");
			logger.severe(e.toString());
		}
	}

	public static void main(String[] args) {
		String executablePath = args[0];
		int numMaples = Integer.parseInt(args[1]);
		String intermediatePrefix = args[2];
		String inputDirectory = args[3];

		Maple maple = new Maple();
		maple.run(executablePath, numMaples, intermediatePrefix, inputDirectory);
		return;
	}
}