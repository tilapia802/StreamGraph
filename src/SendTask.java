package pgps;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.text.*;
import java.io.*;

public class SendTask {

  private static final String TASK_QUEUE_NAME = "schedule_queue";

  public static void main(String[] argv) throws Exception {
    /* Connection setting */
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    /* Queue declare */
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

    JedisPool pool = new JedisPool("zeus02");
    Jedis jedis;
    jedis = pool.getResource();

    BufferedReader in = new BufferedReader(new FileReader("/home/tiffanykuo/graph_testcase/change"));
    String line = "";
    String message = "";
    String batch_message = "";
    int count = 0;
    while((line = in.readLine()) != null){
        int old_state = Integer.valueOf(jedis.get(line.split(" ")[2]));
        int new_distance = 0;
        if (old_state == 2147483647)
            new_distance = 2147483647;
        else
            new_distance = old_state + Integer.valueOf(line.split(" ")[4]);

        message = line.split(" ")[3] + " " + String.valueOf(new_distance) + ";";   
        batch_message = batch_message + message;
        count = count + 1;
	if (count % 10000 == 0)
		System.out.println(count);
        //System.out.println("send " + message);
        //channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
    }
    channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, batch_message.getBytes("UTF-8"));
    System.out.println("[SendTask] Sent");
    
    /* Send out the message */
    //channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
    //System.out.println("[SendTask] Sent '" + message + "'");

    /* Close the connection */
    channel.close();
    connection.close();
  }

  private static String getMessage(int i) {
    int src = 336800;
    String src_str = String.valueOf(src);
    /* Get the time now */
    Date date_now = new Date( );
    SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss:SSS");
    /* Join the string to message to be sent */
    //String message = src_str + " " + ft.format(date_now)+ " " + "ShortestPath";
    String message = "";
    if(i==0)
	message = "483928 2;";
    else if(i==1)
        message = "461 1;";
    else if (i==2)
        message = "560727 2;";
    else if (i==3)
        message = "491517 1;";
    else if (i==4)
        message = "326622 1;";
    else if (i==5)
        message = "150048 3;";
    else if (i==6)
        message = "100720 4;";
    else if (i==7)
        message = "142555 3;";
    else if (i==8)
        message = "351098 2;";
    else if (i==9)
        message = "341026 1;";

    //String message = src_str + " 0;"; 
    return message;
  }

}
