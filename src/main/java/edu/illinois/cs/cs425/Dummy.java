package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
  * This is a dummy maple for testing
  **/

public class Dummy{
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	public static void main(String args[]){
		Socket client =null;
		DataInputStream input=null;
		DataOutputStream output=null;

		if(args.length<4){
			logger.severe("lack of argument\n Dummy <executable path> <number of mappers> <intermediate filename prefix> <input file or directory>");
			return;
		}
		StringBuffer sb = new StringBuffer();
		for (int i = 0;i < 4; i++) {
		    sb.append(args[i]);
		    sb.append(" ");
		}
		String s = sb.toString();
		try{
			client = new Socket("localhost",6679);
			input = new DataInputStream(client.getInputStream());
			output = new DataOutputStream(client.getOutputStream());
			if(args[4].equals("mapJob")) {
				System.out.println("Doing mapJob");
				output.writeUTF("mapJob "+s);
			} else {
				System.out.println("Doing reduceJob");
				output.writeUTF("reduceJob "+s);
			}
		}catch(Exception e){
			logger.severe(e.toString());
		}finally{
			try{
				if (client != null)
				    client.close();
				if (input != null)
				    input.close();
				if (output != null)
				    output.close();
			}catch(Exception e){
				logger.info(e.toString());
			}
		}		
	}
}
