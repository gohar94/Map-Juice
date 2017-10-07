package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJTaskChecker extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    boolean finished;

    public MJTaskChecker() {
        finished = false;
    }

    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.finest("Stopping");
        finished = true;
        logger.finest("Stopped");
    }

    /**
     * This function periodically checks if any tasks assigned to workers have timed out and need to be reassigned 
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted() && !finished) {
            try {
                logger.info("Checking if any tasks have timed out");
                Thread.sleep(10000); // TODO adjust timeout period
                for (Map.Entry<String,MJTask> entry : Daemon.allTasks.entrySet()) {
                    MJTask task = entry.getValue();
                    checkTask(task);
                }
            } catch(Exception e) {
                logger.severe(e.toString());
            }
        }
    }

    public void checkTask(MJTask task) {
        // check if task is not completed
        if (task.isCompleted) {
            return;
        }

        // check if task has timed out
        if (task.isTimedOut()) {
            // check if worker is dead
            if (!Daemon.members.contains(task.workerAssigned)) {
                // remove files produced by the worker, if any
                for (String file : task.filesWritten) {
                    logger.info("Deleting file " + file);
                    SDFSDelete del = new SDFSDelete(Daemon.master_list, "sdfsFile/"+file);
                    try {
                        del.start();
                        del.join();
                    } catch(Exception e) {
                        logger.severe(e.toString());
                        logger.severe("Deletion thread could not be started");
                    }
                    logger.info("File deleted");
                }
                task.filesWritten.clear();
                task.workerAssigned = "";
                task.timeAssigned = null;
                logger.info("Putting this task back in the queue");
                Daemon.tasks.offer(task);
            } else {
                logger.info("Task is taking long but worker is still alive");
            }
        }
    }
}
