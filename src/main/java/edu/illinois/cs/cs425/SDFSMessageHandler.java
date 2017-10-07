package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.IOUtils;

/**
 * This thread is for handling SDFS messages received by other SDFS nodes. 
 */
public class SDFSMessageHandler extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    CopyOnWriteArrayList<String> members;
    CopyOnWriteArrayList<String> files;
    
    Socket socket;
    DataInputStream input;
    DataOutputStream output;
    FileOutputStream fos;
    FileInputStream fis;


    public SDFSMessageHandler(Socket _socket, CopyOnWriteArrayList<String> _members, CopyOnWriteArrayList<String> _master_list, CopyOnWriteArrayList<String> _files) {
        members = _members;
        master_list = _master_list;
        files = _files;
        socket = _socket;
        input = null;
        output = null;
        fos = null;
        fis = null;
    }

    /**
     * This function deletes files from master list
     */
    public void deleteFileFromList(CopyOnWriteArrayList<String> files, String filename) {
        logger.finest("Looking for filename to delete " + filename);
        for (String str : files) {
            logger.finest("File list member inserting is " + str);
            if (str.split("//")[0].equals(filename)) {
                files.remove(str);
                logger.finest("DELETED!");
            }
        }
    }

    /**
     * This function checks if the files list contains a given file
     */
    public boolean containsFile(CopyOnWriteArrayList<String> files, String filename) {
        logger.finest("Looking for filename " + filename);
        for (String str : files) {
            logger.finest("File list member is " + str);
            if (str.split("//")[0].equals(filename)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * This function returns the nodes which contain this file
     */
    public String getFileNodes(CopyOnWriteArrayList<String> files, String filename) {
        String temp = "";
        logger.finest("Gettings nodes for filename " + filename);
        for (String str : files) {
            if (str.split("//")[0].equals(filename)) {
                temp += str.split("//")[1];
                temp += "//";
                temp += str.split("//")[2];
                temp += " ";
            }
        }
        temp = temp.substring(0, temp.length()-1);
        return temp;
    }

    /**
     * This functions returns the file long in a single string
     */
    public String getFilesInSdfs(CopyOnWriteArrayList<String> files, String fileSearch) {
        boolean all = false;
        if (fileSearch.equals("-la")) {
            all = true;
        }
        String ans = "";
        for (String file : files) {
            if (all || fileSearch.equals(file.split("//")[0])) {
                ans += file;
                ans += " ";
            }
        }
        if (ans.length() == 0) {
            return ans;
        }
        ans = ans.substring(0, ans.length()-1);
        return ans;
    }

    /**
     * This function parses the user command and handles it appropriately. 
     */
    public void run() {
        try {
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());

            String message  = input.readUTF();
            logger.finest("Incoming message is " + message);
            
            String command = message.split(" ")[0];
            String[] arguments = Arrays.copyOfRange(message.split(" "), 1, message.split(" ").length);

            if (command.equals("write")) {
                // TODO Maintain a list of files on this node
                fos = new FileOutputStream(arguments[0]);
                IOUtils.copy(input, fos);
                fos.flush();
                logger.finest("Writing file " + arguments[0] + " into local SDFS directory");
                
                // Check if file is written successfully
                File temp = new File(arguments[0]);
                if (temp.exists()) {
                    if (temp.isFile()) {
                        logger.finest("File written!");
                        notifyMasters(arguments[0]);
                    } else {
                        logger.severe("File not written - it does not exist on SDFS!");
                    }
                } else {
                    logger.severe("File not written - it does not exist on SDFS!");
                }
            } else if (command.equals("read")) {
                // Send the file
                fis = new FileInputStream(arguments[0]);
                IOUtils.copy(fis, output);
                output.flush();
            } else if (command.equals("delete")) {
                File temp = new File(arguments[0]);
                boolean success = temp.delete();
                logger.finest("Status of file " + arguments[0] + " deletion is " + success);
            } else if (command.equals("getWhere")) {
                String fileName = arguments[0];
                String response = "abort";
                if (containsFile(files, fileName)) {
                    response = getFileNodes(files, fileName);
                }
                output.writeUTF(response);
                logger.finest("getWhere reply sent = " + response);
            } else if (command.equals("getWhereD")) { // this one is for deleting
                String fileName = arguments[0];
                String response = "abort";
                if (containsFile(files, fileName)) {
                    response = getFileNodes(files, fileName);
                }
                output.writeUTF(response);
                logger.finest("getWhere reply sent = " + response);
                deleteFileFromList(files, fileName);
            } else if (command.equals("putWhere")) {
                // A node will ask the master where to write the file
                String fileName = arguments[0];
                if (containsFile(files, fileName)) {
                    // TODO deny the request to write file, it is already in SDFS
                    logger.finest("File " + fileName + " is already in SDFS!");
                    output.writeUTF("abort");
                } else {
                    String[] temp = new String[members.size()];
                    temp = members.toArray(temp);
                    ArrayList<String> copy = new ArrayList<String>(Arrays.asList(temp)); // Copy the array to remove member from it
                    ArrayList<String> chosen = new ArrayList<String>();
                    // Add self node to members for this operation
                    String self = Daemon.localhostName.getHostName()+"//"+Integer.toString(Daemon.listenerPort)+"//"+Daemon.timeStamp+"//"+Daemon.sdfsPort;
                    logger.finest("Added self to list " + self);
                    copy.add(self);
                    while (!copy.isEmpty() && chosen.size() != 3) {
                        int location = ThreadLocalRandom.current().nextInt(0, copy.size());
                        logger.finest("location is " + location);
                        chosen.add(copy.get(location));
                        logger.finest("location guy is " + copy.get(location));
                        copy.remove(location);
                    }
                    String response = "";
                    for (String str : chosen) {
                        response += str + " ";
                    }
                    response = response.substring(0, response.length()-1);
                    output.writeUTF(response);
                    logger.finest("putWhere reply sent = " + response);
                }
            } else if (command.equals("listEntry")) {
                // A node will tell master to update its list when file is written
                // Format is fileName//IP//port
                arguments = arguments[0].split("//");
                logger.finest("About to add file in list");
                if(!files.contains(arguments[0]+"//"+arguments[1]+"//"+arguments[2])){
                    files.add(arguments[0]+"//"+arguments[1]+"//"+arguments[2]);
                }
                logger.finest("Done adding file " + arguments[0] + " at " + arguments[1] + ":" + arguments[2]);
            } else if (command.equals("ls")) {
                output.writeUTF(getFilesInSdfs(files, arguments[0]));
            } else if (command.equals("master")){
                //TODO if I am the master, prepared to receive log file, else add target to list
                String name = arguments[0].split("//")[0];
                String id = arguments[0].split("//")[2];
                if(name.equals(InetAddress.getLocalHost().getHostName()) && Daemon.timeStamp.equals(id)&&!master_list.contains(arguments[0])){
                    logger.finest("Oh my god I am elected to be a master Ahhhhhh");
                    String file_Line = null;
                    file_Line = input.readUTF();
                    files.add(file_Line);
                    Daemon.master = true;
                }
                if(!master_list.contains(arguments[0])){
                    master_list.add(arguments[0]);
                }
                
            } else if(command.equals("manDown")){

                String failMember = arguments[0];
                logger.finest("Informed by Pinger: "+ failMember+" is Down.");
                logger.finest("Top master start handling failure of "+failMember);
                if(master_list.size()<3 && members.size()>2){
                    logger.finest("Top master start election process "+" [Target] "+failMember);
                    //LeaderElection leaderElection  = new LeaderElection(master_list,members,files);
                    //leaderElection.start();
                    //leaderElection.join();
                    logger.finest("Election process complete");
                }  
                logger.finest("Top master start renewal process [Target] "+failMember);   
                ReplicaRenewal rRenewal= new ReplicaRenewal(members,master_list,failMember,files);
                rRenewal.start();
                rRenewal.join();  
                logger.finest("Renewal process complete");  

            }else if(command.equals("clear")){
                String failMember= arguments[0].split("//")[0]+arguments[0].split("//")[3];
                logger.finest("Common member start clear file list. [Down member] "+failMember);              
                for(String file:files){
                    String nameInFile = file.split("//")[1]+file.split("//")[2];
                    if(nameInFile.equals(failMember)){
                        logger.finest("Delete file "+file);
                        if(files.contains(file)){
                            files.remove(file);
                        }else{
                            logger.finest("File does not exist");
                        }
                        logger.finest("Delete complete");
                    } 
                }
                logger.finest("Clear process complete");
            }else if(command.equals("renew")){
                //Format: "renew pieces place"
                String place = arguments[1];
                String sdfsFile = arguments[0];
                int dstPort = Integer.parseInt(place.split("//")[1]);
                String dstIP = place.split("//")[0];         
                logger.finest("Start send "+sdfsFile+"\n"+"Master find place "+"IP: "+dstIP+"Port: "+Integer.toString(dstPort));
                Socket renewSocket = new Socket(dstIP,dstPort);
                //renewSocket.setSoTimeout(10000);
                DataOutputStream dstOutput  = new DataOutputStream(renewSocket.getOutputStream());
                dstOutput.writeUTF("write " + sdfsFile);
                dstOutput.flush();
                logger.finest("Send write "+sdfsFile+" sucess");
                FileInputStream renewFis = new FileInputStream(sdfsFile);
                IOUtils.copy(renewFis, dstOutput);
                dstOutput.flush();
                logger.finest("write "+sdfsFile+"for renewal sucess");
                output.writeUTF("sucess");
                output.flush();
            }else {
                logger.finest("Unknown SDFS message!");
            }
            logger.finest("Operation completed");
        } catch(Exception e) {
            logger.severe(e.toString());
        } finally {
            try {
                if (fos != null)
                    fos.close();
                if (fis != null)
                    fis.close();
                input.close();
                output.close();
                socket.close();
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }

    /**
     * This method will notify all masters that this node has written a file to its SDFS directory
     */
    public void notifyMasters(String file) {
        try {
            for (String master : this.master_list) {
                notifyMaster(master, file);
            }
        } catch (Exception e) {
            logger.severe(e.toString());
        }
    }

    /**
     * Given a master node, this method will send him confirmation that file has been written to SDFS
     */
    public void notifyMaster(String master, String file) {
        DataOutputStream output = null; 
        Socket socket = null;
        try {
            String[] info = master.split("//");
            String ip = info[0];
            int port = Integer.parseInt(info[3]);
            Random rand = new Random();
            int randomNum = rand.nextInt((9 - 0) + 1) + 0;
            socket = new Socket(info[0], port+randomNum);
            output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF("listEntry " + file + "//" + InetAddress.getLocalHost().getHostName()  + "//" + Daemon.sdfsPort);
        } catch (Exception e) {
            logger.severe(e.toString());
        } finally {
            try {
                if (output != null)
                    output.close();
                if (socket != null)
                    socket.close();
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
