package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockBasedHeapQuickSelectSketch extends HeapQuickSelectSketch {

	private UpdateSketch delegatee_;
	private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock();
	
	public LockBasedHeapQuickSelectSketch() {
		
		super(Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES), DEFAULT_UPDATE_SEED, (float) 1.0, ResizeFactor.X8,
				Family.QUICKSELECT.getMinPreLongs(), Family.QUICKSELECT);

		UpdateSketchBuilder usb = new UpdateSketchBuilder(); 
		delegatee_ = usb.build();
	}

	@Override
	public UpdateReturnState update(final long dataItem) {
		try {
			lock_.writeLock().lock();
			return delegatee_.update(dataItem);
		} finally {
			lock_.writeLock().unlock();
		}
	}

	@Override
	public double getEstimate() {
		try {
			lock_.readLock().lock();
			return delegatee_.getEstimate();
		} finally {
			lock_.readLock().unlock();
		}
	}

//	@Override
//	public void reset() {
//
//		this.delegatee_ = HeapUpdateDoublesSketch.newInstance(k_);
//
//	}

}
