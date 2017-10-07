package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This thread is for the master to listen to all incoming messages and take action.
*/
public class MJListener extends Thread {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	ServerSocket listener;
	Boolean finished;

	public MJListener(int portNumber) {
		try {
			listener = new ServerSocket(portNumber);
			finished = false;
		} catch (Exception e) {
			logger.info("Could not start MJ Master Listener");
			logger.severe(e.toString());
		}
	}

	/**
	 * This function is used to gracefully stop this thread if needed.
	 */
	public void stopMe() {
	    logger.info("Stopping");
	    finished = true;
	    logger.info("Stopped");
	}

	public void run() {
		while (!Thread.currentThread().isInterrupted() && !finished) {
			try {
				Socket socket = listener.accept();
				logger.info("Connected to " + socket.getRemoteSocketAddress());

				logger.info("Spawning thread to handle incoming request to MJ Listener");
				MJMessageHandler handler = new MJMessageHandler(socket);
				handler.start();
			} catch (Exception e) {
				logger.severe(e.toString());
			}
		}
	}
}
