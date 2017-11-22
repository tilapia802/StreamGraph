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
  public static void main(String[] argv) throws Exception {
    pgps.ReadConf readconf = new pgps.ReadConf();
    pgps.Logger logger = new pgps.Logger(readconf.getLogDirectory()+"Scheduler_log");
    pgps.SchedulerMessageQueue scheduler_message_queue = new pgps.SchedulerMessageQueue();

    ExecutorService executor = Executors.newFixedThreadPool(12);

    executor.submit(new ReceiveMessage(readconf, logger, scheduler_message_queue));
    executor.submit(new MyTask(readconf, logger, scheduler_message_queue));
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
  private static MyTask scheduler_task;
  int first_message = 1;
  int worker_num = 0;
  public ReceiveMessage(pgps.ReadConf readconf, pgps.Logger logger, pgps.SchedulerMessageQueue scheduler_message_queue)throws Exception{
    this.readconf = readconf;
    this.worker_num = readconf.getWorkerCount();
    this.logger = logger;
    this.scheduler_message_queue = scheduler_message_queue;
    this.scheduler_task = new MyTask(readconf, logger, scheduler_message_queue);
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
        String [] message_split = message.split(";");
        logger.log("[Scheduler] Received message");
        try{
          if (first_message == 1){
            scheduler_message_queue.pushToQueue(message_split[0]);
            first_message = 2;
          }
          else{
            if (first_message == 2){
              /* Split the outgoing vertices equally to workers */
              String out_vertex = message.split(" ")[1];
              //System.out.println("out vertex is" + out_vertex);
              String out_vertex_split[] = out_vertex.split(",");
              int length = out_vertex_split.length;
              int cut = length/worker_num;
              int workerID = 1;
              //System.out.println("length is " + length);
              for(int i=0;i<length;i++){
                //System.out.println("i is " + i);
                //System.out.println("workerID is " + workerID);
                //System.out.println("out vertex is " + out_vertex_split[i]);
                scheduler_task.worker_vertex_data[workerID][Integer.valueOf(out_vertex_split[i])] = 1;
                if(i == (cut * workerID - 1) && workerID!=worker_num)
                  workerID = workerID + 1;
              }
              first_message = 3;
            }
            /* Split the message */
            for (int i=0;i<message_split.length;i++){
              //message_split[i] = 1 2,3,4 time algo
              String single_message_split[] = message_split[i].split(" "); 
              String out_vertex = single_message_split[1]; //2,3,4
              String out_vertex_split[] = out_vertex.split(",");
              for (int j=0;j<out_vertex_split.length;j++){
                String message_task = single_message_split[0] + " " + out_vertex_split[j] + "," + " " + single_message_split[2] + " " + single_message_split[3];
                //message_task = 1 2, time algo
                scheduler_message_queue.pushToQueue(message_task);
              }
            }
          }
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

}
class MyTask implements Runnable {
  private static final String EXCHANGE_NAME = "directTOworker";
  pgps.ReadConf readconf;
  pgps.Logger logger;
  private int batch_size;
  private int total_vertex_num;
  int worker_num;
  int worker_load []; //Record load(task count) for each worker
  int worker_vertex_data [][]; //The data that each worker has
  pgps.SchedulerMessageQueue scheduler_message_queue;
  ConnectionFactory factory;
  Connection connection_tracker ;
  Channel channel_tracker ;

  ConnectionFactory[] factory_worker;
  Connection[] connection_worker;
  Channel[] channel_worker;

  /* For profiling */
  int responsible_worker [];
  int percentage_locality = 0;
  int percentage_all = 0;
  long scheduler_time = 0;
 
  public MyTask(pgps.ReadConf readconf, pgps.Logger logger, pgps.SchedulerMessageQueue scheduler_message_queue)throws Exception{
    this.readconf = readconf;
    this.logger = logger;
    this.scheduler_message_queue = scheduler_message_queue;
    this.batch_size = readconf.getBatchSize();
    this.worker_num = readconf.getWorkerCount();
    this.total_vertex_num = readconf.getVertexNumber();
    this.worker_load = new int [worker_num+1];
    this.worker_vertex_data = new int [worker_num+1][total_vertex_num+1];
    this.responsible_worker = new int [worker_num+1];

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
    int worker_counter = 1;
    int worker_num = readconf.getWorkerCount();
    int delay_time = readconf.getDelayTime();
    int workerID = 1;
    String message_batch_worker [] = new String[worker_num+1]; //Store the message(task) for each Worker
    String message_batch_tracker [] = new String[worker_num+1]; //Store the message to Graph Tracker
    /* Initialization of String array */
    for(int i=0;i<worker_num+1;i++){
      message_batch_worker[i] = "";
      message_batch_tracker[i] = "";
    }
    int batch_counter_worker [] = new int [worker_num+1];
    int first_message = 1;
    long startTime_worker [] = new long [worker_num+1];
    long endTime = 0;
    long currentDelayTime_worker [] = new long [worker_num+1];
    int [] info_array = new int[2];
    /* Profile */
    long scheduler_start_time = 0;

    while(true){
      /* Get message from message queue */
      String message = scheduler_message_queue.popFromQueue(); // src des, time for split task version
      endTime = System.currentTimeMillis(); 
      
      /* Calculate current delay time */
      for (int i=1;i<=worker_num;i++){
        if (endTime > startTime_worker[i] && startTime_worker[i]!=0)
          currentDelayTime_worker[i] = endTime - startTime_worker[i];
      }  

      /* Got message */
      if (!message.equals("NULL")){
        if(first_message!=1){
          scheduler_start_time = System.currentTimeMillis(); 
          /* Decide which worker ID to assign task according to scheduler policy */
          info_array = SchedulerPolicy(message);
          workerID = info_array[0];
          /* Profile */
          scheduler_time = scheduler_time + System.currentTimeMillis() - scheduler_start_time;
          //System.out.println("schedule time is " + scheduler_time);
          startTime_worker[workerID] = System.currentTimeMillis();
          batch_counter_worker[workerID] = batch_counter_worker[workerID] + 1;
          message_batch_worker[workerID] =  message_batch_worker[workerID] + message + ";";
          if(info_array[1] == 0)
            message_batch_tracker[workerID] =  message_batch_tracker[workerID] + message + ";";
          //如果沒有資料才要塞到message_batch_tracker        
        }
        else{
          message_batch_worker[1] = message_batch_worker[1] + message + ";";
          message_batch_tracker[1] = message_batch_tracker[1] + message + ";";
        }

        /* Check if any batch comes to batch size and send */
        for(int i=1;i<=worker_num;i++){
          if(batch_counter_worker[i] >= batch_size || first_message == 1){
            workerID = i;
            try{
              /* Check if it needs to send subgraph request to graph tracker */
              if(message_batch_tracker[i].length()>0) 
                sendSubgraphRequest(message_batch_tracker[i],workerID,logger,channel_tracker);//Send subgraph request to graph tracker
              sendWork(message_batch_worker[i],workerID,logger,readconf, channel_worker); //Send task to worker 
            }catch(Exception e){}
            //System.out.println("Send message " + message_batch_worker[i]);
            batch_counter_worker[i] = 0;
            message_batch_worker[i] = "";
            message_batch_tracker[i] = "";
            currentDelayTime_worker[i] = 0;
            startTime_worker[i] = 0;
            first_message = 0;
          }
        }
      }
      /* If batch is not empty and comes to delay time -> send */
      for(int i=1;i<=worker_num;i++){
        if (batch_counter_worker[i]!=0 && currentDelayTime_worker[i] >= delay_time){ //If it has message to send and comes to delay time
          workerID = i;
          try{
            /* Check if it needs to send subgraph request to graph tracker */
            if(message_batch_tracker[i].length()>0) 
              sendSubgraphRequest(message_batch_tracker[i],workerID,logger,channel_tracker);//Send subgraph request to graph tracker
            sendWork(message_batch_worker[i],workerID,logger,readconf,channel_worker); //Send task to worker 
          }catch(Exception e){}
          batch_counter_worker[i] = 0;
          message_batch_worker[i] = "";
          message_batch_tracker[i] = "";
          currentDelayTime_worker[i] = 0;
          startTime_worker[i] = 0;
        }
      }
      //System.out.println("percentage of locality is " + percentage_locality);
      //System.out.println("percentage of all is " + percentage_all);
    }  
  }
  
  private int[] SchedulerPolicy(String message){
    //info_array[0] = workerID, info_array[1] = 1(has data, don't need subgraph, otherwise 0)
    int [] info_array = new int [2];
    int vertex = Integer.valueOf(message.split(" ")[1].split(",")[0]); //The task vertex
    //Start from worker_has_data[0], record that which worker has data, 
    //ex: worker_has_data[0]=2(worker2), worker_has_data[1]=4(worker4)
    int worker_has_data [] = new int [worker_num+1]; 
    int count = 0;
    int workerID=0;
    /* Check which worker has this vertex data according to worker_vertex_data array */
    for(int i=1;i<=worker_num;i++){
      if(worker_vertex_data[i][vertex] == 1){
        //System.out.println("worker"+i+"has data" + vertex);
        worker_has_data[count] = i;
        count = count + 1;
      }
    }
    //System.out.println("count is " + count);
    //System.out.println("responsible vertex for worker 1 is " + responsible_worker[1]);
    //System.out.println("responsible vertex for worker 2 is " + responsible_worker[2]);
    //percentage_all = percentage_all + 1;

    if (count == 1){ //Only one worker has data
      //percentage_locality = percentage_locality + 1;
      workerID = worker_has_data[0];
      worker_load[workerID] = worker_load[workerID] + 1; 
      worker_vertex_data[workerID][vertex] = 1;
      info_array[0] = workerID;
      info_array[1] = 1;
      return info_array;
    }
    else if (count == 0 || count == worker_num){ //No worker or all the workers have data
      workerID = 1;
      /* Find the workerID with less work load */
      for(int i=1;i<worker_num;i++){
        if (worker_load[i+1] < worker_load[i])
          workerID = i+1;
      }
      /* After deciding workerID, we have to update its load and responsible vertex array, too */
      worker_load[workerID] = worker_load[workerID] + 1; 
      if(count == 0){
        worker_vertex_data[workerID][vertex] = 1;
        //responsible_worker[workerID] = responsible_worker[workerID] + 1;
      }
      /* Set up the return value */
      info_array[0] = workerID;
      if (count == 0)
        info_array[1] = 0;
      else 
        info_array[1] = 1;
      return info_array;
    }
    else{
      workerID = worker_has_data[0];
      for(int i=1;i<=worker_num+1;i++){
        if(worker_has_data[i]!=0){
          if (worker_load[worker_has_data[i]] < worker_load[worker_has_data[i-1]]){
            workerID = worker_has_data[i];
          }
        }
      }
      worker_load[workerID] = worker_load[workerID] + 1; 
      worker_vertex_data[workerID][vertex] = 1;
      //responsible_worker[workerID] = responsible_worker[workerID] + 1;
      info_array[0] = workerID;
      info_array[1] = 1;
      return info_array;
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
