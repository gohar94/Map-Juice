package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MJJob {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	String executablePath;
    String id;
	ArrayList<MJTaskMapper> tasks;
    ArrayList<MJTaskReducer> tasks2;
    String type;

	public MJJob(String _type, String _executablePath, ArrayList<MJTaskMapper> _tasks, ArrayList<MJTaskReducer> _tasks2) {
		executablePath = _executablePath;
        id = executablePath;
		tasks = _tasks;
        for (MJTaskMapper task : tasks)
            task.job = this;
        tasks2 = _tasks2;
        for (MJTaskReducer task : tasks2)
            task.job = this;
        type = _type;
	}

    public void unpack() {
        logger.info("Unpacking job");
        int tasksAdded = 0;
        for (MJTask task : tasks) {
            if (Daemon.allTasks.containsKey(task.id)) {
                logger.info("Task already present in queue, skipping over");
                continue;
            }
            logger.info("Added task");
            task.print();
            Daemon.tasks.add(task);
            Daemon.allTasks.put(task.id, task);
            tasksAdded++;
        }
        for (MJTask task : tasks2) {
            if (Daemon.allTasks.containsKey(task.id)) {
                logger.info("Task already present in queue, skipping over");
                continue;
            }
            logger.info("Added task");
            task.print();
            Daemon.tasks.add(task);
            Daemon.allTasks.put(task.id, task);
            tasksAdded++;
        }
        logger.info("Unpacked job - number of tasks added is " + tasksAdded);
    }

    public void print() {
        System.out.println("Executable Path is " + executablePath);
        
        System.out.println("Tasks in this job are:");
        for (MJTask task : tasks) {
            task.print();
        }
        for (MJTask task : tasks2) {
            task.print();
        }
    }
}
