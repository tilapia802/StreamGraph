package pgps;
import com.rabbitmq.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import java.text.*;

public class Worker {

  public static Map<String, String> graph_topology_map;
  StampedLock lock = new StampedLock();
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
    //Jedis jedis = pool.getResource();
    //jedis.set("1","0");
    //jedis.close();
    
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
  long wait_time = 0;
  long message_timet = 0;
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
          //System.out.println("receive task " + message);
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
    String new_distance = "";
    String [] message_split;
    String [] message_batch_split = message.split(";");
   
    int i;
    String tasklist;
    //String [] tasklist = new String [2];
    //tasklist[0] = "";
    //tasklist[1] = "";
    String new_message = "";
    boolean local_task = false;
    for(int j=0;j<message_batch_split.length;j++){
      //System.out.println("message batch split is " + message_batch_split[j]);
      how_many_batch = how_many_batch + 1;
      //System.out.println("How many batch is " + how_many_batch);
      message_split = message_batch_split[j].split(" ");
      vertexID = message_split[0];
      //logger.log("vertexID is " + vertexID);
      new_distance = message_split[1];       
      //logger.log("new_distance is " + new_distance);
      //logger.log("old_distance is " + jedis.get(vertexID));
      int weight;
    
      /* See if new distance is shorter than the distance now */
      if (Integer.valueOf(new_distance) < Integer.valueOf(jedis.get(vertexID))){
        logger.log("update");
        jedis.set(vertexID,String.valueOf(new_distance));

        /* Find neighbors of this vertex */
        while(true){
          //System.out.println("hi");
          synchronized(worker.graph_topology_map){
            if(worker.graph_topology_map.containsKey(vertexID))
              break;
          }
          try{
            Thread.sleep(3);
          }catch (Exception e){}
          //System.out.println("waiting for " + vertexID);
        }

        synchronized(worker.graph_topology_map){
          tasklist = worker.graph_topology_map.get(vertexID);
        }
        if(tasklist.equals("")){ //has no outgoing neighbors
          continue;
        }
        String [] tasklist_split = tasklist.split(" ");
        for (i=0;i<tasklist_split.length;i+=2){
          local_task = false;
          synchronized(worker.graph_topology_map){
            if (worker.graph_topology_map.containsKey(tasklist_split[i])){
                local_task = true;
            }
          }
          new_message = tasklist_split[i] + " " + String.valueOf(Integer.valueOf(new_distance) + Integer.valueOf(tasklist_split[i+1]));
          if(local_task == true){
            //System.out.println("push to local queue " + new_message);
            worker_message_queue_receive.pushToQueue(new_message + ";");
          }
          else{
            //System.out.println("push to remote queue " + new_message);
            worker_message_queue.pushToQueue(new_message);
          }
        }
      }   
    }
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
            //message_count = message_count + message.getBytes().length;
            //System.out.println("Message is " + message);
            //logger.log("[Worker] Worker " + workerID + " received subgraph");
            String [] message_split = message.split(",");
            int len = message_split.length; //How many task batch
            //System.out.println("len is " + len);
            /* Subgraph format: src out1 weight out2 weight, src2 out1 weight out weight
               Ex : 12 4 40 9 48 12 0 13 18 17 7, 4 2 38 4 0 9 37 10 32 12 40            */
            String map_key; //12
            String map_value; //4:40,9:48,12:0,13:18,17:7
            String [] message_split_split;
            String meta_data; //Add meta data for experiment
            int key_len;
            for (int i=0;i<len;i++){
              key_len = 0;
              if(message_split[i].equals(""))
                break;
              for(int j=0;j<10;j++){
                if(message_split[i].charAt(j) == ' ')
                  break;
                else
                  key_len = key_len + 1;
              } 
              //message_split_split = message_split[i].split(" ");
              //map_key = message_split_split[0]; 
              map_key = message_split[i].substring(0,key_len);
              //meta_data = message_split_split[message_split_split.length-1];
              //map_value要扣掉meta data的資料
              //Add meta data
              //map_value = message_split[i].substring(map_key.length()+1,message_split[i].length()-(meta_data.length()+1));
              //origin: 
              map_value = message_split[i].substring(key_len+1);
              //change
              synchronized(worker.graph_topology_map){
                  worker.graph_topology_map.put(map_key,map_value);
              }
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
    int first = 0;
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
            //logger.log("batch " + message_batch);
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
            //logger.log("delay: " + message_batch);
            batch_counter = 0;
            message_batch = "";
            currentDelayTime = 0;
            startTime = 0;
          }
          catch(Exception e){}
      }
    }
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
        //logger.log("[Worker] Worker " + workerID + " received task " + message);
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
