package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This thread is for checking the job/tasks queues and assigning workers if there are any.
 */
public class MJAssigner extends Thread {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	Boolean finished;

	public MJAssigner() {
		finished = false;
	}

	/**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }

    /**
     * This function is used to choose a worker from the list of free workers
     * If the worker is not alive, it will try to pick another one until the list is empty.
     */
    public String pickWorker() {
        String temp = "";
        while (!Daemon.workers.isEmpty()) {
            temp = Daemon.workers.get(0);
            Daemon.workers.remove(0);
            if (Daemon.members.contains(temp)) {
                return temp;
            }
        }
        return null;
    }

	public void run() {
		while (!Thread.currentThread().isInterrupted() && !finished) {
			try {
				Thread.sleep(1000); // TODO adjust timeout
				Boolean noWorkers = false;
				while (!Daemon.workers.isEmpty()) {
					while (!Daemon.tasks.isEmpty()) {
                        // logger.info("Tasks and workers available");
						if (Daemon.workers.isEmpty()) {
                            // logger.info("No workers available");
							noWorkers = true;
							break;
						}
                        String worker = pickWorker();
                        if (worker == null) {
                            // logger.info("No workers available");
                            noWorkers = true;
                            break;
                        }
						MJTask task = Daemon.tasks.poll();
                        if (task.isCompleted) {
                            // logger.info("Task " + task.id + " is already completed");
                            Daemon.workers.add(worker);
                        } else {
                            logger.info("Assigning task");
                            task.assign(worker);
                            // logger.info("Assigning success");
                            //Daemon.tasks.offer(task); // Put the task at the back of the queue
                        }

					}
					if (!Daemon.jobs.isEmpty()) {
						MJJob job = Daemon.jobs.poll();
                        logger.info("Unpacking job");
                        job.print();
                        job.unpack();
					}
					if (noWorkers) {
                        logger.info("No workers available");
						break;
					}
				}
			} catch (Exception e) {
				logger.severe(e.toString());
			}
		}
	}
}
