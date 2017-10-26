package com.yahoo.sketches.theta;

import java.util.HashMap;

public class WorkerThread extends Thread {

	HashMap<Object, AContex> map_ = new HashMap<Object, AContex>();

	public WorkerThread() {
		super();
	}

	protected ContextTheta getThetaContex(MWMRHeapQuickSelectSketch sk) {
		
		AContex act = getSketch(sk);
		if (act != null) {
			assert (act instanceof ContextTheta); 
			return (ContextTheta) act;
		}
		else {
			ContextTheta ct = new ContextTheta(sk);
			PutToMap(sk, ct);
			return ct;
		}
	}
	
	
	private AContex getSketch(Object sk) {
		return map_.get(sk);
	}
	

	private void PutToMap(Object sk, AContex ct) {
		map_.put(sk, ct);
	}

}
