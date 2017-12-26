import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class RunRedis {
  public static void main(String[] argv) throws Exception{
    /* Initialization of redis */
    pgps.ReadConf readconf = new pgps.ReadConf();
    String master_hostname = readconf.getMasterHostname();
    JedisPool pool = new JedisPool(master_hostname);
    //Jedis jedis = new Jedis(master_hostname);			
		//System.out.println("Connection to server sucessfully");
    //System.out.println("Server is running: "+jedis.ping());
    int vertex_num = readconf.getVertexNumber();
    int thread_num = 16;
    ExecutorService executor = Executors.newFixedThreadPool(thread_num);

    /* Initialization before execution / Get result after execution */
	  if (argv[0].equals("init")){
      System.out.println("init");
      int cut = (vertex_num+1) / thread_num ;
      for (int i=0;i<thread_num;i++){
        if (i == thread_num-1)
          executor.submit(new ParallelWrite(i*cut, vertex_num+1, pool));
        else
          executor.submit(new ParallelWrite(i*cut, (i+1)*cut, pool));
      }
		  executor.shutdown();
	  }
	  else if (argv[0].equals("result")){
      Jedis jedis = pool.getResource();
		  for(int i=0;i<vertex_num+1;i++){
       	System.out.println(i + " " + jedis.get(String.valueOf(i)));
      }
      jedis.close();
		}       
  }
}
class ParallelWrite implements Runnable {
  int start;
  int end;
  Jedis jedis;
  public ParallelWrite(int start, int end, JedisPool pool)throws Exception{
    this.start = start;
    this.end = end;
    this.jedis = pool.getResource();
  }
  @Override
  public void run(){
    String value = "";
    for(int i=start;i<end;i++){
      //System.out.println(i);
      if (i%10000==0){System.out.println(i);}
      value = jedis.get(String.valueOf(i));
      if(!value.equals("200000"))
          jedis.set(String.valueOf(i), "200000");
    }
    jedis.close();
  }
}
