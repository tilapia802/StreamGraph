package pgps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReadConf {

    private String master_hostname;
    private int worker_count;
    private String[] worker_hostname;
    private String input_filepath;
    private int vertex_num;
    private int edge_num;
    private String log_directory;
    private int batch_size;
    private int delay_time;
   
    public ReadConf() throws FileNotFoundException {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("/home/tiffanykuo/git-repos/StreamingGraphProcessingSystem/pgps.conf"));
            JSONObject jsonObject = (JSONObject) obj;
            this.master_hostname = (String) jsonObject.get("MasterHostname");
            this.worker_count = Integer.parseInt((String) jsonObject.get("WorkerNumber"));
            String worker_hostnames = (String) jsonObject.get("WorkerHostnames");
            this.worker_hostname = worker_hostnames.split(",");
            this.input_filepath = (String) jsonObject.get("InputFilePath");
            this.log_directory = (String) jsonObject.get("LogDirectory");
            this.batch_size = Integer.parseInt((String) jsonObject.get("BatchSize"));
            this.delay_time = Integer.parseInt((String) jsonObject.get("DelayTime"));
            BufferedReader in = new BufferedReader(new FileReader(this.input_filepath));
            String line;
            line = in.readLine();
            vertex_num = Integer.valueOf(line.split(" ")[0]);
            edge_num = Integer.valueOf(line.split(" ")[1]);
        }
        catch(Exception e){}   
    }
    
    /* Return number of worker nodes */
    public int getWorkerCount() {
        return this.worker_count;
    } 
    /* Return hostname of master node */
    public String getMasterHostname(){
        return this.master_hostname;
    }
    /* Return hostname of worker node */
    public String getWorkerHostname(int workerID){
        return this.worker_hostname[workerID-1];
    }
    /* Return input file path */
    public String getInputFilepath() {
        return this.input_filepath;
    } 
    /* Return log directory path */
    public String getLogDirectory() {
        return this.log_directory;
    } 
    /* Return total number of vertex */
    public int getVertexNumber() {
        return this.vertex_num;
    } 
    /* Return total number of edge */
    public int getEdgeNumber() {
        return this.edge_num;
    } 
    /* Return number of batch size */
    public int getBatchSize() {
        return this.batch_size;
    } 
    /* Return delat time */
    public int getDelayTime() {
        return this.delay_time;
    }
}
