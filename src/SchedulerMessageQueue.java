package pgps;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SchedulerMessageQueue{
	private ConcurrentLinkedQueue<String> queue; 
	public SchedulerMessageQueue(){
		this.queue = new ConcurrentLinkedQueue<String>();
	}
	public int getQueueSize(){
		return this.queue.size();
	}
	public void pushToQueue(String message){
		synchronized(queue){
			this.queue.offer(message);
		}
	}
	public String popFromQueue(){
		//return this.queue.poll();
		synchronized(queue){
    		if(!queue.isEmpty()){
       			return this.queue.poll();
    		}
			else
				return "NULL";
		}
	}
}