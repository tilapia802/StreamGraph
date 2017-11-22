package pgps;
import com.rabbitmq.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;
import java.util.*;
import java.text.*;

public class Worker {

  public static Map<String, String> graph_topology_map;
  ReentrantLock lock = new ReentrantLock();
  public static void main(String[] argv) throws Exception {
    pgps.ReadConf readconf = new pgps.ReadConf();
    pgps.Logger logger = new pgps.Logger(readconf.getLogDirectory() + "Worker" + String.valueOf(argv[0]) + "_log");
    pgps.WorkerMessageQueue worker_message_queue = new pgps.WorkerMessageQueue();
    pgps.WorkerMessageQueue worker_message_queue_receive = new pgps.WorkerMessageQueue();
    
    /* Error handling: user doesn't give argument (workerID) */
    if (argv.length < 1){
      System.err.println("Usage: Worker [workerID] ");
      System.exit(1);
    }

    /* Initial array for graph vertex status */
    JedisPool pool = new JedisPool(readconf.getMasterHostname());
    Jedis jedis = pool.getResource();
    jedis.set("1","0");
    jedis.close();
    
    /* The hashmap that stores graph topology */
    graph_topology_map = new HashMap<String, String>();

    ExecutorService executor = Executors.newFixedThreadPool(12);
    /* Thread that receive message(task) from scheduler */
    executor.submit(new WorkerReceiveMessage(readconf, logger, worker_message_queue_receive, argv[0]));
    /* Thread that do task computation */
    executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));     
    executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));
    executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));
    executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));
    executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));     
    //executor.submit(new WorkerTask(readconf, logger, worker_message_queue, worker_message_queue_receive, argv[0], pool));     
    /* Thread that receive subgraph information */
    executor.submit(new WorkerReceiveSubgraphTask(readconf, logger, argv[0], pool));
    /* Thread that send task to Scheduler */
    executor.submit(new WorkerSendToScheduler(readconf, logger, worker_message_queue));
    executor.submit(new WorkerSendToScheduler(readconf, logger, worker_message_queue));
    
    executor.shutdown();

  }

}
class WorkerTask implements Runnable {
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.WorkerMessageQueue worker_message_queue;
  pgps.WorkerMessageQueue worker_message_queue_receive;
  private static Worker worker;
  String workerID;
  Jedis jedis;
  String queueName;
  long threadId;
  int how_many_batch = 0;
  int how_many_local_task = 0;
  public WorkerTask(pgps.ReadConf readconf, pgps.Logger logger, pgps.WorkerMessageQueue worker_message_queue, pgps.WorkerMessageQueue worker_message_queue_receive, String workerID, JedisPool pool)throws Exception{
    worker = new Worker();
    this.workerID = workerID;
    this.readconf = readconf;
    this.logger = logger;
    this.worker_message_queue = worker_message_queue;
    this.worker_message_queue_receive = worker_message_queue_receive;
    this.jedis = pool.getResource();
  }

  @Override
  public void run() {  
    this.threadId = Thread.currentThread().getId();
    System.out.println("ThreadID is " + threadId);      
    /* Get messages from queue*/
    while(true){   
      String message = worker_message_queue_receive.popFromQueue(); 
      if (!message.equals("NULL")){
        try{
          doWork(message,jedis,worker.graph_topology_map,workerID,logger,readconf,worker_message_queue_receive,threadId);   
          //System.out.println("local task is " + how_many_local_task);
          //System.out.println("How many batch is " + how_many_batch);
          logger.log("[Worker] Worker " + workerID + " done doing task");
        }catch(Exception e){} 
      }        
    }    
  }

  private void doWork(String message, Jedis jedis, Map<String, String> graph_topology_map, String workerID, pgps.Logger logger, pgps.ReadConf readconf, pgps.WorkerMessageQueue worker_message_queue_receive, long threadId)throws Exception {
    /* Parse message from scheduler */
    String vertexID = "";
    String senderID = "";
    String message_time,algname;
    String [] message_split = message.split(" ");
    String [] message_batch_split = message.split(";");
   
    int i;
    //StringBuilder tasklist = new StringBuilder();
    String [] tasklist = new String [2];
    tasklist[0] = "";
    tasklist[1] = "";
    String [] graph_topology_value_split;
    String graph_topology_value;
    String message_to_scheduler="";

    /* If vertexID = 1, which is source vertex of shortest path */
    if (message_split.length == 3){ //src vertex (first message)
      how_many_batch = how_many_batch + 1;
      vertexID = message_split[0];
      message_time = message_split[1];
      algname = message_split[2].substring(0,message_split[2].length()-1);

      /* Wait until it has subgraph data */ 
      while (!worker.graph_topology_map.containsKey(vertexID)){
        try{
          Thread.sleep(8);
        }catch (Exception e){}
      }
      //logger.log("[Worker] Worker " + workerID + " finished waiting for subraph value of key 1");
      tasklist = getOutgoingNeighbors(vertexID,graph_topology_map,logger);
      //logger.log("outgoing neighbors of vertexID" + vertexID + "is" + tasklist);
      
      /* Send tasklist to scheduler */
      if (tasklist[0].length() > 0){ 
        try{
          message_to_scheduler = vertexID + " " + tasklist[0].toString() + " " + message_time + " " + algname; //src des,des,des,des,des time shortestpath
          worker_message_queue.pushToQueue(message_to_scheduler);
          //sendTaskToScheduler(message_to_scheduler,vertexID,tasklist.toString(),message_time,algname,workerID,logger,readconf);
        }
        catch(Exception e){}
      }
    }
    else{ //other vertex
      for(int j=0;j<message_batch_split.length;j++){
        how_many_batch = how_many_batch + 1;
        //System.out.println("How many batch is " + how_many_batch);
        message_split = message_batch_split[j].split(" ");
        senderID = message_split[0];
        vertexID = message_split[1]; //des,des,des,
        message_time = message_split[2];
        algname = message_split[3];
        int new_distance;
        int weight;
        /*if(threadId == 22){
          try{logger.log("Start waiting");}catch (Exception e){}
        }*///profile
        /*while (!worker.graph_topology_map.containsKey(String.valueOf(senderID))){
          try{
            Thread.sleep(8);
          }catch (Exception e){}
        }*/
        /*if(threadId == 22){
          try{logger.log("Finish waiting");}catch (Exception e){}
        }*///profile
        //logger.log("[Worker] Worker " + workerID + " finished waiting for subraph value of key " + senderID);
        //System.out.println("senderID=" + senderID + " vertexID=" + vertexID);

        String[] vertexID_split = vertexID.split(",");
        int length = vertexID_split.length;
        /*if(threadId == 22){
          try{logger.log("Start computing");}catch (Exception e){}
        }*///profile
        for(i=0;i<length;i++){
          vertexID = vertexID_split[i];
          //weight = getEdgeWeight(senderID,vertexID,graph_topology_map);
          new_distance = Integer.valueOf(jedis.get(senderID)) + 1;//weight;
          
          /* See if new distance is shorter than the distance now */
          if (new_distance < Integer.valueOf(jedis.get(vertexID))){
            //System.out.println("[Worker] " + vertexID + "update value from " + jedis.get(vertexID) + " to " + new_distance);
            jedis.set(vertexID,String.valueOf(new_distance));
            
            /*if(threadId == 22){
              try{logger.log("In Start waiting");}catch (Exception e){}
            }*///pofile
            /* Find neighbors of this vertex */
            while (!worker.graph_topology_map.containsKey(vertexID)){
              try{
                Thread.sleep(8);
              }catch (Exception e){}
            }
            /*if(threadId == 22){
              try{logger.log("In Finish waiting");}catch (Exception e){}
            }*///profile
            //logger.log("[Worker] Worker " + workerID + " finished waiting for subraph value of key " + vertexID);
            tasklist = getOutgoingNeighbors(vertexID,graph_topology_map,logger);

            /* Check if it has neighbors */
            if (tasklist[0].length() > 0){ 
              try{
                message_to_scheduler = vertexID + " " + tasklist[0].toString() + " " + message_time + " " + algname; //src des,des,des,des,des time shortestpath
                worker_message_queue.pushToQueue(message_to_scheduler);
                //sendTaskToScheduler(message_to_scheduler,vertexID,tasklist.toString(),message_time,algname,workerID,logger,readconf);
              }
              catch(Exception e){       
              }
            }
            String localtask = "";
            if (tasklist[1].length()>0){
              //System.out.println("self task vertex is " + tasklist[1]);
              String [] localtask_vertex_split = tasklist[1].split(","); 
              how_many_local_task = how_many_local_task + localtask_vertex_split.length;
              for (int k=0;k<localtask_vertex_split.length;k++){
                localtask = localtask + vertexID + " " + localtask_vertex_split[k] + " " + message_time + " " + algname + ";";
              }
              //System.out.println("local task is " + localtask);
              worker_message_queue_receive.pushToQueue(localtask);
            }
          }
        }
      }
    }
  }  

  private static String[] getOutgoingNeighbors(String vertexID, Map<String, String> graph_topology_map, pgps.Logger logger) throws Exception{
    String graph_topology_value; 
    String [] graph_topology_value_split;
    //StringBuilder tasklist = new StringBuilder();
    String [] tasklist = new String [2];
    tasklist[0] = "";
    tasklist[1] = "";
    int i;
    synchronized(worker.graph_topology_map){
      graph_topology_value = worker.graph_topology_map.get(vertexID);
    }
    if (graph_topology_value.equals(":1,")){
      //tasklist.append("");
      return tasklist;
    }
    /* Split the graph topology of vertexID and find outgoing neighbors and construct tasklist to be sent */
    graph_topology_value_split = graph_topology_value.split(",");
    for(i=0;i<graph_topology_value_split.length;i++){
      String outgoing_vertex = graph_topology_value_split[i].split(":")[0];
      if (vertexID.equals(outgoing_vertex)) 
        continue;
      synchronized(worker.graph_topology_map){
        if (worker.graph_topology_map.containsKey(outgoing_vertex)){
          tasklist[1] = tasklist[1] + outgoing_vertex;
          tasklist[1] = tasklist[1] + ",";
          continue;
        }
      }
      //tasklist.append(graph_topology_value_split[i].split(":")[0]);
      //tasklist.append(",");
      tasklist[0] = tasklist[0] + outgoing_vertex;
      tasklist[0] = tasklist[0] + ",";
    }
    return tasklist; //des,des,des,des
  }

  private static int getEdgeWeight(String srcID, String desID, Map<String, String> graph_topology_map){
    String graph_topology_value; 
    String [] graph_topology_value_split;
    StringBuilder tasklist = new StringBuilder();
    int i;
    int weight = 0;
    synchronized(worker.graph_topology_map){
      graph_topology_value = worker.graph_topology_map.get(String.valueOf(srcID));
    }
    /* Split the graph topology of srcID and find weight of desID */
    graph_topology_value_split = graph_topology_value.split(",");
    for(i=0;i<graph_topology_value_split.length;i++){
      if (graph_topology_value_split[i].split(":")[0].equals(desID)){
        weight = Integer.valueOf(graph_topology_value_split[i].split(":")[1]);
        break; 
      }
    }
    return weight;
  }

  private void sendTaskToScheduler(String message, String senderID, String tasklist, String message_time, String algname, String workerID, pgps.Logger logger, pgps.ReadConf readconf ) throws Exception{
    /* Connection setting */
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(readconf.getMasterHostname());
    factory.setAutomaticRecoveryEnabled(true);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    /* Queue declare */
    channel.queueDeclare("schedule_queue", true, false, false, null);
    channel.basicPublish("", "schedule_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
    channel.close();
    connection.close();

  }

};
class WorkerReceiveSubgraphTask implements Runnable {
  private static final String EXCHANGE_NAME2 = "Tracker_directTOworker";
  ConnectionFactory factory_receiveSubgraph;
  Connection connection_receiveSubgraph;
  Channel channel_receiveSubgraph;
  pgps.ReadConf readconf;
  pgps.Logger logger;
  private static Worker worker;
  String workerID;
  Jedis jedis;
  String queueName_receiveSubgraph;
  int message_count = 0;
  public WorkerReceiveSubgraphTask(pgps.ReadConf readconf, pgps.Logger logger, String workerID, JedisPool pool)throws Exception{
    worker = new Worker();
    this.readconf = readconf;
    this.jedis = pool.getResource();;
    this.logger = logger;
    this.workerID = workerID;
    
    /* Connection setting (thread_receiveSubgraph) */
    factory_receiveSubgraph = new ConnectionFactory();
    factory_receiveSubgraph.setHost("localhost");
    ExecutorService es = Executors.newFixedThreadPool(2);
    connection_receiveSubgraph = factory_receiveSubgraph.newConnection(es);
    channel_receiveSubgraph = connection_receiveSubgraph.createChannel();
    
    /* Exchange declare */
    channel_receiveSubgraph.exchangeDeclare(EXCHANGE_NAME2, "direct");
    queueName_receiveSubgraph = channel_receiveSubgraph.queueDeclare().getQueue();
    
    /* Set routing key for this worker */
    String key_receiveSubgraph = "worker" + workerID;
    channel_receiveSubgraph.queueBind(queueName_receiveSubgraph, EXCHANGE_NAME2, key_receiveSubgraph);
  }

  @Override
  public void run() {
        /* Receive messages */
        Consumer consumer = new DefaultConsumer(channel_receiveSubgraph) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
                                     AMQP.BasicProperties properties, byte[] body) throws IOException {
            String message = new String(body, "UTF-8");
            message_count = message_count + message.getBytes().length;
            //System.out.println("Message size is " + message_count);
            //logger.log("[Worker] Worker " + workerID + " received subgraph");
            String [] message_split = message.split(" ");
            int len = message_split.length;
            /* Subgraph format: src,out1:weight,out2:weight, src2,out1:weight,out:weight
               Ex : 12,4:40,9:48,12:0,13:18,17:7, 4,2:38,4:0,9:37,10:32,12:40,           */
            String map_key; //12
            String map_value; //4:40,9:48,12:0,13:18,17:7
            String [] message_split_split;
            String meta_data; //Add meta data for experiment
            for (int i=0;i<len;i++){
              message_split_split = message_split[i].split(",");
              map_key = message_split_split[0]; 
              //meta_data = message_split_split[message_split_split.length-1];
              //map_value要扣掉meta data的資料
              //System.out.println("string is " + message_split[i]);
              //System.out.println("total length = " + message_split[i].length());
              //System.out.println("start index = " + map_key.length()+1);
              //System.out.println("end index = " + String.valueOf(message_split[i].length()-(meta_data.length()+1)));
              //Add meta data
              //map_value = message_split[i].substring(map_key.length()+1,message_split[i].length()-(meta_data.length()+1));
              //origin: 
              map_value = message_split[i].substring(map_key.length()+1);
              //System.out.println("map value is " + map_value);
              synchronized(worker.graph_topology_map){
                  worker.graph_topology_map.put(map_key,map_value);
              }
              //System.out.println("Get" + graph_topology_map.get(map_key));
            }
          }
        };
        try{
         channel_receiveSubgraph.basicConsume(queueName_receiveSubgraph, true, consumer);}
        catch(Exception e){}     
      }

};
class WorkerSendToScheduler implements Runnable{
  ConnectionFactory factory;
  Connection connection;
  Channel channel;
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.WorkerMessageQueue worker_message_queue;
  private static int batch_size;
  private static Worker worker;
  public WorkerSendToScheduler(pgps.ReadConf readconf, pgps.Logger logger, pgps.WorkerMessageQueue worker_message_queue)throws Exception{
    worker = new Worker();
    this.readconf = readconf;
    this.logger = logger;
    this.worker_message_queue = worker_message_queue;
    this.batch_size = readconf.getBatchSize();
    //this.batch_size = 1;
    /* Connection setting */
    factory = new ConnectionFactory();
    factory.setHost(readconf.getMasterHostname());
    factory.setAutomaticRecoveryEnabled(true);
    connection = factory.newConnection();
    channel = connection.createChannel();

    /* Queue declare */
    channel.queueDeclare("schedule_queue", true, false, false, null);
    
  }
  @Override
  public void run(){
    int delay_time = readconf.getDelayTime();
    String message_batch = "";
    int batch_counter = 0;
    long startTime = 0;
    long endTime = 0;
    long currentDelayTime = 0;
    int first = 1;
    while(true){
      String message = worker_message_queue.popFromQueue();
      endTime = System.currentTimeMillis();  
      if (endTime > startTime && startTime!=0)
        currentDelayTime = endTime - startTime;

      if (!message.equals("NULL")){
        batch_counter = batch_counter + 1;
        message_batch = message_batch  + message + ";";
        startTime = System.currentTimeMillis();
        if (batch_counter >= batch_size || first == 1){
          try{
            channel.basicPublish("", "schedule_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, message_batch.getBytes("UTF-8"));
            //SendTest(message_batch);
            batch_counter = 0;
            message_batch = "";
            currentDelayTime = 0;
            startTime = 0;
            first = 0;
          }
          catch(Exception e){}
        }
      }
      else if (batch_counter!=0 && currentDelayTime >= delay_time){
        try{
            channel.basicPublish("", "schedule_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, message_batch.getBytes("UTF-8"));
            //SendTest(message_batch);
            batch_counter = 0;
            message_batch = "";
            currentDelayTime = 0;
            startTime = 0;
          }
          catch(Exception e){}
      }
    }
    //channel.close();
    //connection.close();
  }
  private void SendTest(String message)throws Exception{
    /* Connection setting */
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(readconf.getMasterHostname());
    factory.setAutomaticRecoveryEnabled(true);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    /* Queue declare */
    channel.queueDeclare("schedule_queue", true, false, false, null);
    channel.basicPublish("", "schedule_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
    channel.close();
    connection.close();    
        
  }
};
class WorkerReceiveMessage implements Runnable {
  private static final String EXCHANGE_NAME = "directTOworker";
  ConnectionFactory factory;
  Connection connection;
  Channel channel;
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.WorkerMessageQueue worker_message_queue_receive;
  String workerID;
  String queueName;
  public WorkerReceiveMessage(pgps.ReadConf readconf, pgps.Logger logger, pgps.WorkerMessageQueue worker_message_queue_receive, String workerID)throws Exception{
    this.readconf = readconf;
    this.logger = logger;
    this.worker_message_queue_receive = worker_message_queue_receive;
    this.workerID = workerID;
    /* Connection setting (thread_receiveTask) */
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setAutomaticRecoveryEnabled(true);
    ExecutorService es = Executors.newFixedThreadPool(2);
    connection = factory.newConnection(es);
    channel = connection.createChannel();
    /* Exchange declare */
    channel.exchangeDeclare(EXCHANGE_NAME, "direct");
    queueName = channel.queueDeclare().getQueue();
    /* Set routing key for this worker */
    String key = "worker" + workerID;
    channel.queueBind(queueName, EXCHANGE_NAME, key);
    this.logger.log("[Worker] Worker" + workerID + " waiting for messages. To exit press CTRL+C");

  }
  @Override
  public void run() {        
    /* Receive messages */
    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        //logger.log("[Worker] Worker " + workerID + " received task");
        try{ 
          worker_message_queue_receive.pushToQueue(message);
        }
        catch(Exception e){}
      }
    };
    try{
     channel.basicConsume(queueName, true, consumer);}
    catch(Exception e){}
  }
};