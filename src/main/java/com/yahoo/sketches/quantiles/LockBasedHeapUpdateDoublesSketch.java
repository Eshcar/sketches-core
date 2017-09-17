package com.yahoo.sketches.quantiles;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockBasedHeapUpdateDoublesSketch extends HeapUpdateDoublesSketch {

	private HeapUpdateDoublesSketch delegatee_;
	private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock();

	public LockBasedHeapUpdateDoublesSketch(final int k) {
		super(k);
		this.delegatee_ = HeapUpdateDoublesSketch.newInstance(k);
	}

	@Override
	public void update(final double dataItem) {
		try {
			lock_.writeLock().lock();
			delegatee_.update(dataItem);
		} finally {
			lock_.writeLock().unlock();
		}
	}

	@Override
	public double getQuantile(final double fraction) {
		try {
			lock_.readLock().lock();
			return delegatee_.getQuantile(fraction);
		} finally {
			lock_.readLock().unlock();
		}
	}

	@Override
	public void reset() {

		this.delegatee_ = HeapUpdateDoublesSketch.newInstance(k_);

	}
}
