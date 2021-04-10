package deerBase;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWLockManager {
	ReadWriteLock rwLock;
	
	public RWLockManager() {
		rwLock = new ReentrantReadWriteLock();
	}
	
	public synchronized void upgrade() {
		ReentrantLock upgradeLock = new ReentrantLock();
		upgradeLock.lock();
		upgradeLock.unlock();
	}
}
