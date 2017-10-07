package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;

public class MJTask {
	protected final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	String executablePath;
    String id;
    boolean isCompleted;
    Date timeAssigned;
    String workerAssigned;
    ArrayList<String> filesWritten;
    MJJob job;

	public MJTask(String _executablePath) {
		executablePath = _executablePath;
        id = executablePath;
        isCompleted = false;
        timeAssigned = null;
        workerAssigned = "";
        filesWritten = new ArrayList<String>();
        job = null;
	}

    public void assign(String worker) {
        workerAssigned = worker;
        timeAssigned = new Date();
        System.out.println("For worker " + id+ " -- " + timeAssigned.getTime());
        filesWritten.clear();
    }

    public boolean isTimedOut() {
        if (timeAssigned == null) {
            // this job has not been assigned to anyone yet
            logger.info("Time assigned is null " + id);
            return false;
        }
        Date current = new Date();

        try {
            System.out.println(current.getTime());
            System.out.println(timeAssigned.getTime());
            long diff = current.getTime() - timeAssigned.getTime();
            logger.info("Time elapsed since assignment of task " + id + " is " + diff);
            if (diff > 30000) {
                logger.info("Timed out");
                return true;
            }
            return false;
        } catch(Exception e) {
            logger.severe(e.toString());
        }

        return false;
    }

    public void print() {
        System.out.println("ID is " + id);
        System.out.println("Executable Path is " + executablePath);
    }
}
