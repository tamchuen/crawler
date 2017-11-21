package com.tamchuen.crawler.tasks;

import com.tamchuen.crawler.Config;

/**
 * Task definition
 * @author  Dequan
 */
public interface Task
{
	public static final int MAX_TASKIDS = 5000000;


	public String getName();
	/**
	 * init the specified task
	 * @param config
	 * @return
	 */
	public void initialize(Config config);

	/**
	 * start the specified task
	 * @param config
	 * @return
	 */
	public boolean start();

	/**
	 * stop the specified task
	 * @param config
	 * @return
	 */
	public void stop();

	/**
	 * get the status of the specified task
	 * @param config
	 * @return
	 * 
	 * 
	 */
	public TaskStatus getStatus();

	/**
	 * @author  Dequan
	 */
	public enum TaskStatus{
		/**
		 * 
		 * 
		 */
		UNKNOWN,
		/**
		 * 
		 * 
		 */
		INITIALIZED,
		/**
		 * 
		 * 
		 */
		STARTED,
		/**
		 * 
		 * 
		 */
		STOPPED
	}
}
