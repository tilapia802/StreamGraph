package pgps;
import com.rabbitmq.client.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.text.*;
import java.io.*;
import org.apache.commons.lang3.StringUtils;

public class GraphTracker {

  public static int[][] worker_data;
  ReentrantLock lock = new ReentrantLock();

  public static void main(String[] argv) throws Exception {
    pgps.ReadConf readconf = new pgps.ReadConf();
    pgps.Logger logger = new pgps.Logger(readconf.getLogDirectory() + "GraphTracker_log");
    pgps.GraphTrackerMessageQueue graphtracker_message_queue = new pgps.GraphTrackerMessageQueue();
    
    logger.log("Start initial graph");
    /* Read graph topology from input file */
    String[] graph_table = initialGraph(readconf);
    logger.log("After initial graph");
    int vertex_num = readconf.getVertexNumber();  //total number of vertex
    int worker_num = readconf.getWorkerCount();
  
    /* Record which graph topology data worker has */
    worker_data = new int[worker_num+1][vertex_num+1];

    ExecutorService executor = Executors.newFixedThreadPool(12);
    executor.submit(new GraphTrackerTask(readconf, logger, graphtracker_message_queue, graph_table));
    executor.submit(new GraphTrackerTask(readconf, logger, graphtracker_message_queue, graph_table));
    executor.submit(new GraphTrackerTask(readconf, logger, graphtracker_message_queue, graph_table));
    
    executor.submit(new GraphTrackerReceiveMessage(readconf, logger, graphtracker_message_queue));
    
    executor.shutdown();
  
  }

  private static String[] initialGraph(pgps.ReadConf readconf) throws IOException{
    BufferedReader in = new BufferedReader(new FileReader(readconf.getInputFilepath()));
    String line;
    line = in.readLine();
    int vertex_num = readconf.getVertexNumber();
    int edge_num = readconf.getEdgeNumber();

    String[] graph_table = new String[vertex_num+1];
    for (int i=0;i<vertex_num+1;i++){
      graph_table[i] = "null"; 
    }
    System.out.println("after initilization");
    String [] line_split;
    String des = "";
    int src = 0;
    int count = 0;
    int src_end = 0;
    while((line = in.readLine()) != null){
      if (count % 10000 == 0)
          System.out.println(count);
      ///line_split = line.split(" ");
      ///src = Integer.parseInt(line_split[0]);
      src_end = String.valueOf(count).length();
      src = Integer.parseInt(line.substring(0,src_end)); 
      //For twitter
      
      if(line.length()==src_end){
        graph_table[src] = des;
        count = count + 1;
        continue;
      }
      
      graph_table[src] = line.substring(src_end+1,line.length());
      //int weight = 1;
      //graph_table[src] = des;
      count = count + 1;
      
    }

    /* Initial graph table */
    /*int[][] graph_table = new int[vertex_num+1][edge_num+1 ];
    for (int i=0;i<vertex_num+1;i++){
      for (int j=0;j<edge_num+1;j++){
        if(i == j)
          graph_table[i][j] = 0;
        else
          graph_table[i][j] = 200000;
      }
    }*/
    /* Put initial graph (topology) in graph table (vertex starts from ID 1) */
    /*while((line = in.readLine()) != null)
    {
      String [] line_split = line.split(" ");
      int src = Integer.parseInt(line_split[0]);
      int des = Integer.parseInt(line_split[1]);
      int weight = Integer.parseInt(line_split[2]);
      graph_table[src][des] = weight;
      graph_table[des][src] = weight;
    }*/
    in.close();
    return graph_table;
  }
}
class GraphTrackerTask implements Runnable {
  private static final String EXCHANGE_NAME = "Tracker_directTOworker";
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.GraphTrackerMessageQueue graphtracker_message_queue;
  String[] graph_table;
  //private static int[][] worker_data;
  private GraphTracker graphtracker;
  
  ConnectionFactory[] factory;  
  Connection[] connection_worker;
  Channel[] channel_worker;

  public GraphTrackerTask(pgps.ReadConf readconf, pgps.Logger logger, pgps.GraphTrackerMessageQueue graphtracker_message_queue, String[] graph_table)throws Exception{
    graphtracker = new GraphTracker();
    this.readconf = readconf;
    this.logger = logger;
    this.graph_table = graph_table;
    this.graphtracker_message_queue = graphtracker_message_queue;
    int worker_num = readconf.getWorkerCount();

    factory = new ConnectionFactory[worker_num+1];
    connection_worker = new Connection[worker_num+1];
    channel_worker = new Channel[worker_num+1];
    for(int i=1;i<=worker_num;i++){
      factory[i] = new ConnectionFactory();
      factory[i].setHost(readconf.getWorkerHostname(i)); //get hostname of worker
      factory[i].setAutomaticRecoveryEnabled(true);
      connection_worker[i] = factory[i].newConnection();
      channel_worker[i] = connection_worker[i].createChannel();
      channel_worker[i].exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    }
  }

  @Override
  public void run(){
    long threadId = Thread.currentThread().getId();
    System.out.println("Thread ID is " + threadId);
    while(true){
      String message = graphtracker_message_queue.popFromQueue();
      if(!message.equals("NULL")){
        String message_subgraph;
        int workerID = Integer.valueOf(message.substring(message.length()-1));
        int vertex_num = readconf.getVertexNumber();

        /* Get the vertex list that graph tracker needs to send to worker */
        String subgraph_vertex_list = getSubgraphVertex(message, workerID, graph_table);
        
        /* Construct subgraph message according to vertex list  */
        message_subgraph = getSubgraph(subgraph_vertex_list,graph_table,vertex_num);

        try{
          if(!message_subgraph.equals("")){
            sendtoWorker(message_subgraph,workerID,logger,readconf, channel_worker); //Send subgraph to worker 
          }
          /*if(threadId == 22){
              try{logger.log("Send message to worker");}catch(Exception e){}
          }*///profile
        }
        catch(Exception e){}
      }
    } 
  }

  private String getSubgraphVertex(String message, int workerID, String[] graph_table){
    String subgraph_vertex_list="";
    String[] graph_table_split;
   
    if(message.split(" ").length == 4){ //first message
      subgraph_vertex_list = message.split(" ")[0];
      synchronized(graphtracker.worker_data){
        graphtracker.worker_data[workerID][Integer.valueOf(subgraph_vertex_list)] = 1; //Record that this worker will has this vertex data 
      }
    }
    else{
      String [] batch_message_split = message.split(";");
      for(int i=0;i<batch_message_split.length-1;i++){ 
        message = batch_message_split[i];
        String des_str = message.split(" ")[1];    //des,des,des
        /*int src = Integer.valueOf(message.split(" ")[0]);
        synchronized(graphtracker.worker_data){
          if (graphtracker.worker_data[workerID][src] == 0){
            subgraph_vertex_list = subgraph_vertex_list + message.split(" ")[0] + ",";
            graphtracker.worker_data[workerID][src] = 1;
          }
        }*/
        /*String[] des_str_split = des_str.split(",");
        for (int j=0;j<des_str_split.length;j++){
          int des = Integer.valueOf(des_str_split[j]);
          synchronized(graphtracker.worker_data){
            if (graphtracker.worker_data[workerID][des] == 0){  //Worker don't have that vertex data
              subgraph_vertex_list = subgraph_vertex_list + des_str_split[j] + ",";
              graphtracker.worker_data[workerID][des] = 1;
            }
          }
        }*/
        //int vertex = Integer.valueOf(des_str.substring(0,des_str.length()-1));
        //graph_table_split = graph_table[vertex].split(" "); //outgoing neighbors有誰
        subgraph_vertex_list = subgraph_vertex_list + des_str;
        /*if (!graph_table_split[0].equals("")){
          for(int j=0;j<graph_table_split.length;j++){
            subgraph_vertex_list = subgraph_vertex_list + graph_table_split[j] + ",";
            if (j==2)
              break;
          }
        }*/
      }
    }
    return subgraph_vertex_list; // src,des,des,des,des
  }

  private String getSubgraph(String subgraph_vertex_list, String[] graph_table, int vertex_num) {
    if (subgraph_vertex_list.equals("")){
      return "";
    }
    String[] subgraph_vertex_list_split = subgraph_vertex_list.split(","); //我要誰的vertex資訊 //src,des,des,des
    int senderID = 0;
    int vertexID = 0;
    String message_subgraph = ""; //src,out1:weight,out2:weight, src2,out1:weight,out:weight
    
    //String meta_data = StringUtils.repeat("1", 10);
    //System.out.println("meta data is " + meta_data);
    //try{
      //System.out.println("meta data size is " + meta_data.getBytes("UTF-8").length);
    //}catch (Exception e){}
    for (int i=0;i<subgraph_vertex_list_split.length;i++){
      String[] graph_table_split = graph_table[Integer.valueOf(subgraph_vertex_list_split[i])].split(" "); //outgoing neighbors有誰
      String vertex_subgraph = ""; //String of each vertex subgraph in vertex list
      for (int j=0;j<graph_table_split.length;j++){
        vertex_subgraph = vertex_subgraph + graph_table_split[j] + ":1,";
      } 
      vertex_subgraph = subgraph_vertex_list_split[i] + "," + vertex_subgraph; //src,vertex_subgraph     
      //Add meta data
      //vertex_subgraph = vertex_subgraph + meta_data + ",";

      message_subgraph = message_subgraph + vertex_subgraph + " ";
    }
    return message_subgraph;
  }
  private void sendtoWorker(String message, int workerID, pgps.Logger logger, pgps.ReadConf readconf, Channel[] channel_worker) throws Exception {
    String key = "worker" + String.valueOf(workerID); //routing key
    String message_worker = message;
    channel_worker[workerID].basicPublish(EXCHANGE_NAME, key, null, message_worker.getBytes("UTF-8"));
  }

};
class GraphTrackerReceiveMessage implements Runnable {
  private static final String TASK_QUEUE_NAME = "graphtracker_queue";
  private static final String EXCHANGE_NAME = "Tracker_directTOworker";
  ConnectionFactory factory;
  Connection connection;
  Channel channel;
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.GraphTrackerMessageQueue graphtracker_message_queue;
  public GraphTrackerReceiveMessage(pgps.ReadConf readconf, pgps.Logger logger, pgps.GraphTrackerMessageQueue graphtracker_message_queue)throws Exception{
    this.readconf = readconf;
    this.logger = logger;
    this.graphtracker_message_queue = graphtracker_message_queue;
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setAutomaticRecoveryEnabled(true);
    ExecutorService es = Executors.newFixedThreadPool(2);
    connection = factory.newConnection(es);
    channel = connection.createChannel();
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    channel.basicQos(1);
  }
  @Override
  public void run(){
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    final Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        //logger.log("[GraphTracker] Received");
        try{
            graphtracker_message_queue.pushToQueue(message);
        }
        finally {
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };
    try{
      channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }
    catch (Exception e){}
  }

}
