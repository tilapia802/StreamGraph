package pgps;
import com.rabbitmq.client.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.text.*;
import java.io.*;

public class Scheduler {

  private static final String TASK_QUEUE_NAME = "schedule_queue";
  private static final String EXCHANGE_NAME = "directTOworker";
  public static int worker_load [];
  public static int worker_vertex_data [][];
  public static int first_message;
  public static void main(String[] argv) throws Exception {
    pgps.ReadConf readconf = new pgps.ReadConf();
    pgps.Logger logger = new pgps.Logger(readconf.getLogDirectory()+"Scheduler_log");
    int batch_size = readconf.getBatchSize();
    int worker_num = readconf.getWorkerCount();
    int total_vertex_num = readconf.getVertexNumber();
    first_message = 1;

    pgps.SchedulerMessageQueue scheduler_message_queue = new pgps.SchedulerMessageQueue();
    pgps.SchedulerMessageQueue scheduler_message_queue_worker = new pgps.SchedulerMessageQueue();

    worker_load = new int [worker_num+1]; //Record load(task count) for each worker
    worker_vertex_data  = new int [worker_num+1][total_vertex_num+1]; //The data that each worker has

    ExecutorService executor = Executors.newFixedThreadPool(5);
    executor.submit(new ReceiveMessage(readconf, logger, scheduler_message_queue));
    executor.submit(new MyTask(worker_num, scheduler_message_queue, scheduler_message_queue_worker));
    executor.submit(new MyTask(worker_num, scheduler_message_queue, scheduler_message_queue_worker));
    executor.submit(new SchedulerSendToWorker(readconf, logger, scheduler_message_queue_worker));
    //executor.submit(new MyTask(readconf, logger, scheduler_message_queue));
    
    executor.shutdown();

  }

}
class ReceiveMessage implements Runnable {
  private final static String TASK_QUEUE_NAME = "schedule_queue";
  ConnectionFactory factory;
  Connection connection;
  Channel channel;
  pgps.ReadConf readconf;
  pgps.Logger logger;
  pgps.SchedulerMessageQueue scheduler_message_queue;
  //private static MyTask scheduler_task;
  private static Scheduler scheduler;
  
  int first_message = 0;
  int worker_num = 0;
  //profile
  long split_time = 0;
  int count = 0;
  public ReceiveMessage(pgps.ReadConf readconf, pgps.Logger logger, pgps.SchedulerMessageQueue scheduler_message_queue)throws Exception{
    this.scheduler = new Scheduler();
    this.readconf = readconf;
    this.worker_num = readconf.getWorkerCount();
    this.logger = logger;
    this.scheduler_message_queue = scheduler_message_queue;
    //this.scheduler_task = new MyTask(readconf, logger, scheduler_message_queue);
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setAutomaticRecoveryEnabled(true);
    ExecutorService es = Executors.newFixedThreadPool(3);
    connection = factory.newConnection(es);
    channel = connection.createChannel();
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    logger.log("[Scheduler] Waiting for messages. To exit press CTRL+C");
    channel.basicQos(1);
  }
  @Override
  public void run(){
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");  
    final Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        //System.out.println("receive message " + message);
        //long split_time_start = System.currentTimeMillis();
        //count = count + 1;
        //System.out.println(count);
        String [] message_split = message.split(";");
        logger.log("[Scheduler] Received message ");
        try{
          if (first_message == 1){
            scheduler_message_queue.pushToQueue(message_split[0]);
            first_message = 2;
          }
          else{ 
            /* Split the message */      
            for (int i=0;i<message_split.length;i++){
              if(message_split[i].equals(""))
                break;
              //message_split[i] = 1 2,3,4, time algo
              //message_split[i] = 1(task vertex) 20(new shortest weight) time algo NEW

              scheduler_message_queue.pushToQueue(message_split[i]);

              /*String single_message_split[] = message_split[i].split(" "); 
              String out_vertex = single_message_split[1]; //2,3,4
              String out_vertex_split[] = out_vertex.split(",");
              for (int j=0;j<out_vertex_split.length;j++){
                String message_task = single_message_split[0] + " " + out_vertex_split[j] + "," + " " + single_message_split[2] + " " + single_message_split[3];
                //message_task = 1 2, time algo
                scheduler_message_queue.pushToQueue(message_task);
                //System.out.println("Push " + message_task);
              }*/
            }
            
          }
          //split_time = split_time + System.currentTimeMillis() - split_time_start;
          //System.out.println(split_time);
        }
        finally {
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };
    try{
      channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }
    catch (Exception e){

    }
  }

};
class MyTask implements Runnable {
  private static Scheduler scheduler;
  pgps.SchedulerMessageQueue scheduler_message_queue;
  pgps.SchedulerMessageQueue scheduler_message_queue_worker;
  int worker_num;
  int worker_has_data []; 
  public MyTask(int worker_num, pgps.SchedulerMessageQueue scheduler_message_queue, pgps.SchedulerMessageQueue scheduler_message_queue_worker)throws Exception{
    scheduler = new Scheduler();
    this.scheduler_message_queue = scheduler_message_queue;
    this.scheduler_message_queue_worker = scheduler_message_queue_worker;
    this.worker_num = worker_num;
    this.worker_has_data = new int [worker_num+1];
  }
  @Override
  public void run(){ 
    String info_str = "";
    while(true){ 
      /* Get message from message queue */
      String message = scheduler_message_queue.popFromQueue(); // des weight for new message version

      /* Got message */
      if (!message.equals("NULL")){
        //System.out.println("get message " + message);
        if(scheduler.first_message!=1){
          /* Decide which worker ID to assign task according to scheduler policy */
          info_str = SchedulerPolicy(message);
          scheduler_message_queue_worker.pushToQueue(message + info_str);
        }
        else{
          //System.out.println("receive first message");
          scheduler_message_queue_worker.pushToQueue(message + " 1 0");
          scheduler.first_message = 0;
        }
      }
    }  
  }
  
  private String SchedulerPolicy(String message){
    //info_str = workerID 1/0 (has data, don't need subgraph, otherwise 0)
    String info_str = "";
    int vertex = Integer.valueOf(message.split(" ")[0]); //The task vertex
    
    //Start from worker_has_data[0], record that which worker has data, 
    //ex: worker_has_data[0]=2(worker2), worker_has_data[1]=4(worker4)
    //int worker_has_data [] = new int [worker_num+1]; 
    int count = 0;
    int workerID=0;

    /* Check which worker has this vertex data according to worker_vertex_data array */
    for(int i=1;i<=worker_num;i++){
      synchronized(scheduler.worker_vertex_data){
        if(scheduler.worker_vertex_data[i][vertex] == 1){
          worker_has_data[count] = i;
          count = count + 1;
        }
      }
    }

    if (count == 1){ //Only one worker has data
      workerID = worker_has_data[0];
      worker_has_data[0] = 0;
      synchronized(scheduler.worker_load){ //Record the task load of each workers
        scheduler.worker_load[workerID] = scheduler.worker_load[workerID] + 1; 
      }
      synchronized(scheduler.worker_vertex_data){ //Record that this worker will has data
        scheduler.worker_vertex_data[workerID][vertex] = 1;
      }
      info_str = info_str + " " + workerID + " 1" ; //Worker has data, don't need to send subgraph request 
      return info_str;
    }
    else if (count == 0 || count == worker_num){ //No worker or all the workers have data
      workerID = 1; //Start from worker 1 to check work load later
      /* Find the workerID with less work load */
      for(int i=1;i<worker_num;i++){
        synchronized(scheduler.worker_load){
          if (scheduler.worker_load[i+1] < scheduler.worker_load[i])
            workerID = i+1;
        }
      }
      /* After deciding workerID, we have to update its load and responsible vertex array, too */
      synchronized(scheduler.worker_load){
        scheduler.worker_load[workerID] = scheduler.worker_load[workerID] + 1; 
      }
      if(count == 0){
        synchronized(scheduler.worker_vertex_data){
          scheduler.worker_vertex_data[workerID][vertex] = 1;
        }
      }
      else{
        //Re initial worker_has_data
        for(int i=0;i<count;i++){
          worker_has_data[i] = 0;
        }
      }

      /* Set up the return value */
      if (count == 0)
        info_str = info_str + " " + workerID + " 0";
      else 
        info_str = info_str + " " + workerID + " 1";
      return info_str;
    }
    else{ //Some workers has data (>1, <worker_num)
      workerID = worker_has_data[0];
      for(int i=0;i<worker_num-1;i++){
        if(worker_has_data[i]!=0){
          synchronized(scheduler.worker_load){
            if (scheduler.worker_load[worker_has_data[i]] < scheduler.worker_load[worker_has_data[i+1]]){
              workerID = worker_has_data[i];
            }
          }
          worker_has_data[i] = 0;
        }
        else
          break;
      }
      synchronized(scheduler.worker_load){
        scheduler.worker_load[workerID] = scheduler.worker_load[workerID] + 1; 
      }
      synchronized(scheduler.worker_load){
        scheduler.worker_vertex_data[workerID][vertex] = 1;
      }
      info_str = info_str + " " + workerID + " 1";
      return info_str;
    }
  }


};
class SchedulerSendToWorker implements Runnable{
  private static final String EXCHANGE_NAME = "directTOworker";
  pgps.ReadConf readconf;
  pgps.Logger logger;
  private int batch_size;
  private int total_vertex_num;
  int worker_num;
  pgps.SchedulerMessageQueue scheduler_message_queue_worker;

  String message_batch_worker [];
  String message_batch_tracker [];
  int delay_time;
  int batch_counter_worker [];
  long startTime_worker [];
  long endTime = 0;
  long currentDelayTime_worker [];
  
  ConnectionFactory factory;
  Connection connection_tracker ;
  Channel channel_tracker ;
  ConnectionFactory[] factory_worker;
  Connection[] connection_worker;
  Channel[] channel_worker;

  public SchedulerSendToWorker(pgps.ReadConf readconf, pgps.Logger logger, pgps.SchedulerMessageQueue scheduler_message_queue_worker)throws Exception{
    this.readconf = readconf;
    this.logger = logger;
    this.scheduler_message_queue_worker = scheduler_message_queue_worker;
    this.batch_size = readconf.getBatchSize();
    this.worker_num = readconf.getWorkerCount();
    this.total_vertex_num = readconf.getVertexNumber();
    message_batch_worker = new String[worker_num+1]; //Store the message(task) for each Worker
    message_batch_tracker = new String[worker_num+1]; //Store the message to Graph Tracker
    for(int i=0;i<worker_num+1;i++){
      message_batch_worker[i] = "";
      message_batch_tracker[i] = "";
    }
    batch_counter_worker = new int [worker_num+1];
    startTime_worker = new long [worker_num+1];
    currentDelayTime_worker = new long [worker_num+1];
    delay_time = readconf.getDelayTime();
    /* Set up connection to send subgraph request to graph tracker */
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setAutomaticRecoveryEnabled(true);
    connection_tracker = factory.newConnection();
    channel_tracker = connection_tracker.createChannel();
    /* Queue declare */
    channel_tracker.queueDeclare("graphtracker_queue", true, false, false, null);
    
    factory_worker = new ConnectionFactory[worker_num+1];
    connection_worker = new Connection[worker_num+1];
    channel_worker = new Channel[worker_num+1];
    for(int i=1;i<=worker_num;i++){
      /* Set up connection to send task to workers */
      factory_worker[i] = new ConnectionFactory();
      factory_worker[i].setHost(readconf.getWorkerHostname(i)); //get hostname
      factory_worker[i].setAutomaticRecoveryEnabled(true);
      connection_worker[i] = factory_worker[i].newConnection();
      channel_worker[i] = connection_worker[i].createChannel();
      channel_worker[i].exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    }
  }
  @Override
  public void run(){ 
    int workerID;
    String worker_has_data;
    int length;
    int first_message = 0;
    //profile
    int count_batch = 0;
    int count_delay = 0;
    int subgraph_batch = 0;
    int subgraph_delay = 0;
    while(true){
      /* Get message from message queue */
      String message = scheduler_message_queue_worker.popFromQueue();
      if(!message.equals("NULL")){
        //System.out.println("Got message");
        length = message.length();
        workerID = Integer.valueOf(message.substring(length-3,length-2));
        worker_has_data = message.substring(length-1);
        //System.out.println("worker has data"+worker_has_data);
        message = message.substring(0,length-4);

        message_batch_worker[workerID] = message_batch_worker[workerID] + message + ";";
        batch_counter_worker[workerID] += 1;
        if(startTime_worker[workerID] == 0)
          startTime_worker[workerID] = System.currentTimeMillis();
        if(worker_has_data.equals("0")){
          message_batch_tracker[workerID] = message_batch_tracker[workerID] + message + ";"; 
        }
        
        /* Check if any batch comes to batch size and send */
        if(batch_counter_worker[workerID] >= batch_size || first_message == 1){
          try{
            /* Check if it needs to send subgraph request to graph tracker */
            if(message_batch_tracker[workerID].length()>0){ 
              sendSubgraphRequest(message_batch_tracker[workerID],workerID,logger,channel_tracker);//Send subgraph request to graph tracker
              //logger.log(" batch send " + message_batch_tracker[workerID]);
              //logger.log("batch subgraph count " + subgraph_batch);
            }
            sendWork(message_batch_worker[workerID],workerID,logger,readconf, channel_worker); //Send task to worker 
            //profile
            //logger.log(" batch send " + message_batch_worker[workerID]);
            //count_batch = count_batch + 1;
            //logger.log("batch send count " + count_batch + " message size " + message_batch_worker[workerID].getBytes("UTF-8").length );
            

          }catch(Exception e){}
          batch_counter_worker[workerID] = 0;
          message_batch_worker[workerID] = "";
          message_batch_tracker[workerID] = "";
          currentDelayTime_worker[workerID] = 0;
          startTime_worker[workerID] = 0;
          first_message = 0;
        }
      }
      for(int i=1;i<=worker_num;i++){
          currentDelayTime_worker[i] = System.currentTimeMillis() - startTime_worker[i];
          if (batch_counter_worker[i]!=0 && currentDelayTime_worker[i] >= delay_time){ //If it has message to send and comes to delay time
            workerID = i;
            try{
              /* Check if it needs to send subgraph request to graph tracker */
              if(message_batch_tracker[i].length()>0){
                sendSubgraphRequest(message_batch_tracker[i],workerID,logger,channel_tracker);//Send subgraph request to graph tracker
                //logger.log(" delay send " + message_batch_tracker[workerID]);
                //logger.log("delay subgraph count " + subgraph_batch);
              }
              sendWork(message_batch_worker[i],workerID,logger,readconf,channel_worker); //Send task to worker 
              //profile
              //logger.log(" delay send " + message_batch_worker[workerID]);
              //count_delay = count_delay + 1;
              //logger.log("delay send count " + count_delay + " message size " + message_batch_worker[workerID].getBytes("UTF-8").length);
            
            }catch(Exception e){}
            batch_counter_worker[i] = 0;
            message_batch_worker[i] = "";
            message_batch_tracker[i] = "";
            currentDelayTime_worker[i] = 0;
            startTime_worker[i] = 0;
          }
        }

    }

  }
  private void sendSubgraphRequest(String message, int workerID, pgps.Logger logger, Channel channel_tracker) throws Exception {
    String message_tracker = message + " worker" + String.valueOf(workerID);
    channel_tracker.basicPublish("", "graphtracker_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, message_tracker.getBytes("UTF-8"));
  }
  private void sendWork(String message, int workerID, pgps.Logger logger, pgps.ReadConf readconf, Channel[] channel_worker) throws Exception {
    String key = "worker" + String.valueOf(workerID); //routing key
    String message_worker = message;
    channel_worker[workerID].basicPublish(EXCHANGE_NAME, key, MessageProperties.PERSISTENT_TEXT_PLAIN, message_worker.getBytes("UTF-8"));
  }
}; 

