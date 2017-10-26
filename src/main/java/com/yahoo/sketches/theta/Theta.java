package com.yahoo.sketches.theta;

public interface Theta {
	
	//TODO: add support for the all API  
	
	public double getEstimate();
	
	public UpdateReturnState update(final long datum);

}
