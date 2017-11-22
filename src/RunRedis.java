import redis.clients.jedis.Jedis;
import java.io.*;
public class RunRedis {
    public static void main(String[] argv) throws Exception{
    	pgps.ReadConf readconf = new pgps.ReadConf();
    	String master_hostname = readconf.getMasterHostname();
    	Jedis jedis = new Jedis(master_hostname);			
		System.out.println("Connection to server sucessfully");
      	System.out.println("Server is running: "+jedis.ping());
      	int vertex_num = readconf.getVertexNumber();
      	//System.out.println(argv[1]);
	    if (argv[0].equals("init")){
		    for(int i=0;i<vertex_num+1;i++){
		    		jedis.set(String.valueOf(i), "200000");
		    }
	    }
	    else if (argv[0].equals("result")){
			for(int i=0;i<vertex_num+1;i++){
            	System.out.println(i + " " + jedis.get(String.valueOf(i)));
          	}	
		} 
        
    }
}
