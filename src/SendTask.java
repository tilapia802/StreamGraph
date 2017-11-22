package pgps;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.*;
import java.text.*;

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

    String message;
    message = getMessage();
    /* Send out the message */
    channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
    System.out.println("[SendTask] Sent '" + message + "'");

    /* Close the connection */
    channel.close();
    connection.close();
  }

  private static String getMessage() {
    /* Generate random src ID number */
    //int src = (int)(Math.random()*20);
    int src = 1;
    String src_str = String.valueOf(src);
    /* Get the time now */
    Date date_now = new Date( );
    SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss:SSS");
    /* Join the string to message to be sent */
    String message = src_str + " " + ft.format(date_now)+ " " + "ShortestPath";
    //String message = String.join(src_str," ",ft.format(date_now)," ","ShortestPath");
    return message;
  }

}