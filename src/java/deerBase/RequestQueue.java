package deerBase;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class RequestQueue {
	public List<Request> requests;
	public ReentrantLock mutex;
	
	public void lock() {
		mutex.lock();
	}
	
	public void unlock() {
		mutex.unlock();
	}
	
	public List<Request> getRequests() {
		return this.requests;
	}
}
