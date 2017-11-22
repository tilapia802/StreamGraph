package pgps;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerMessageQueue{
	private ConcurrentLinkedQueue<String> queue; 
	//private LinkedBlockingQueue<String> queue;
	public WorkerMessageQueue(){
		this.queue = new ConcurrentLinkedQueue<String>();
	}
	public int getQueueSize(){
		return this.queue.size();
	}
	public synchronized void pushToQueue(String message){
		//synchronized(this.queue){
			this.queue.offer(message);
		//}
	}
	public String popFromQueue(){
		synchronized(this.queue){
    		if(!this.queue.isEmpty()){
       			return this.queue.poll();
    		}
			else
				return "NULL";
		}
	}
}