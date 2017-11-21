package com.tamchuen.crawler.tasks;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.tasks.Task.TaskStatus;
import com.tamchuen.jutil.j4log.Logger;

/**
 * Task Engine for managing tasks
 * @author Dequan
 *
 */
public class TaskEngine
{
	private static Logger logger = Logger.getLogger("TaskEngine");

	private Map<String, Task> tasks = new HashMap<String, Task>();
	private static TaskEngine instance;

	private TaskEngine(){

	}

	public static TaskEngine getInstance(){
		if( instance == null ){
			instance = new TaskEngine();
		}
		return instance;
	}

	/**
	 * run a specified task
	 * @param taskName
	 */
	public void addTask( Task task, Config config ){
		logger.debug("[TaskEngine] CMD:Add task " + task.getName() );

		if( tasks.containsKey(task.getName()) ){
			logger.warn("[TaskEngine] Task already exists: " + task.getName() );
			Task oldTask = tasks.get( task.getName() );
			// reinit the task
			oldTask.initialize(config); 
		}else{
			tasks.put(task.getName(),  task);
			task.initialize( config );
			logger.debug("[TaskEngine] Task " + task.getName() + " initialized");
		}
	}

	/**
	 * run a specified task
	 * @param taskName
	 */
	public void runTask(String taskName){
		Task task = tasks.get(taskName);
		if( task == null ){
			logger.warn("[TaskEngine] Task not found: " + taskName );
		}else{
			boolean code = task.start();
			logger.debug("[TaskEngine] Task " + taskName + " started with code " + code);
		}
	}

	public void runAll(){
		logger.debug("[TaskEngine] CMD:Run all tasks");
		for( Iterator<String> iter = tasks.keySet().iterator(); iter.hasNext();){
			String taskName = iter.next();
			runTask( taskName );
		}
	}

	/**
	 * stop a specified task
	 * @param taskName
	 */
	public void stopTask(String taskName){
		Task task = tasks.get(taskName);
		if( task == null ){
			logger.warn("[TaskEngine] Task not found: " + taskName );
		}else{
			task.stop();
			logger.debug("[TaskEngine] Task " + taskName + " stopped" );
		}
	}

	public void stopAll(){
		logger.debug("[TaskEngine] CMD:Stop all tasks");
		for( Iterator<String> iter = tasks.keySet().iterator(); iter.hasNext();){
			String taskName = iter.next();
			stopTask( taskName );
		}
	}

	public TaskStatus checkStatus(String taskName){
		Task task = tasks.get(taskName);
		if( task == null ){
			logger.warn("[TaskEngine] Task not found: " + taskName );
			return TaskStatus.UNKNOWN;
		}else{
			return task.getStatus();	
		}
	}
	public Task getTask(String taskName){
		Task task = tasks.get(taskName);
		if( task == null ){
			logger.warn("[TaskEngine] Task not found: " + taskName );
			return null;
		}else{
			return task;
		}
	}
}
