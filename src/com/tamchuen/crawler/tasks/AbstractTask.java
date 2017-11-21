package com.tamchuen.crawler.tasks;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.tamchuen.crawler.Config;

/**
 * Abstract Task definition
 * TODO: General save/load of task queques 
 * @author Dequan
 *
 */
public abstract class AbstractTask implements Task
{ 

	protected final BlockingQueue<String> taskIdQueue = new LinkedBlockingQueue<String>(MAX_TASKIDS);

	protected TaskStatus status;

	protected Config config;

	protected String taskName;

	public void setName(String name){
		this.taskName = name;
	}

	public String getName(){
		return this.taskName;
	}

	public BlockingQueue<String> getQueue(){
		return taskIdQueue;
	}

	public void initialize(Config config){
		this.config = config;
		this.status = TaskStatus.INITIALIZED;
	}


	public boolean start(){
		this.status = TaskStatus.STARTED;
		return true;
	}


	public void stop(){
		this.status = TaskStatus.STOPPED;    
	}
	 public TaskStatus getStatus(){
		 return status;
	 }
	 public void setStatus(TaskStatus status){
		 this.status = status;
	 }

	 public Config getConfig(){
		 return config;
	 }

	 /**
	  * the code for identifying the task running status
	  * @author  Dequan
	  */
	 public enum TaskCode{
		 SUCCESS,
		 HTTP_ERROR,
		 JSON_ERROR,
		 NOT_FOUND,
		 DB_ERROR,
		 DB_DUPLICATE,
		 UNKNOWN,
		 EXISTS
	 }

	 public static String getMsgFromCode(TaskCode code){
		 String msg = "UNKNOWN";
		 switch ( code ){
		 case SUCCESS:
			 msg = "SUCCESS";
			 break;
		 case HTTP_ERROR:
			 msg = "HTTP_ERROR";
			 break;
		 case JSON_ERROR:
			 msg = "JSON_ERROR";
			 break;
		 case NOT_FOUND:
			 msg = "NOT_FOUND";
			 break;
		 case DB_ERROR:
			 msg = "DB_ERROR";
			 break;
		 case DB_DUPLICATE:
			 msg = "DB_DUPLICATE";
			 break;
		 case EXISTS:
			 msg = "EXISTS";
			 break;
		 }
		 return msg;
	 }
	 
	 public void statusLog(String msg){
		 config.getStatusPanel().log(msg);
	 }
	 
	 public void log(String msg){
		 config.getLogPanel().log(msg);
	 }
}
