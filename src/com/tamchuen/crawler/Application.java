package com.tamchuen.crawler;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.UIManager;

import com.tamchuen.crawler.brightkite.tasks.BrightkiteTask;
import com.tamchuen.crawler.gowalla.tasks.GowallaTask;
import com.tamchuen.crawler.gowalla.tasks.SpotEventsTask;
import com.tamchuen.crawler.gowalla.tasks.SpotTask;
import com.tamchuen.crawler.gowalla.tasks.UserFriendTask;
import com.tamchuen.crawler.gowalla.tasks.UserTask;
import com.tamchuen.crawler.gowalla.tasks.UserTopSpotsTask;
import com.tamchuen.crawler.gowalla.tasks.UserVisitedSpotsTask;
import com.tamchuen.crawler.tasks.Task;
import com.tamchuen.crawler.tasks.TaskEngine;
import com.tamchuen.crawler.ui.GuiLogPanel;
import com.tamchuen.crawler.util.GuiUtil;
import com.tamchuen.jutil.j4log.LoggerFactory;
import com.tamchuen.jutil.util.FileUtil;
import com.tamchuen.jutil.util.Pair;


/**
 * Minimal GUI for this application.
 * 
 * @author Dequan
 * Project: Crawler
 * Date : Oct 30, 2011 
 */
public class Application extends JPanel implements WindowListener,ActionListener{
	/**
	 * Serial version
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Newline used in output.
	 */
	public static final String NEWLINE = System.getProperty("line.separator");
	private static final String ABOUT_MSG = "This is yet another crawler created by Dequan Zhou,\n Department of Geomatics Engineering,\n Schulich School of Engineering, \n University of Calgary.";

	/**
	 * Logging output area. 
	 */
	private JFrame frame;
	public GuiLogPanel statusLogPanel;
	public GuiLogPanel taskLogPanel;
	private JButton runButton1;
	private JButton runButton2;
	private JButton runButton3;
	private JButton runButton4;
	private JButton runButton5;
	private JButton aboutButton;
	private JButton setSpotRangeButton;
	private JButton setUserRangeButton;
	private JButton saveTaskQueueButton;
	private JTextField textMinUserId;
	private JTextField textMaxUserId;
	private JTextField textMinSpotId;
	private JTextField textMaxSpotId;
	private JComboBox<String> cbUserIdRanges;
	private JComboBox<String> cbSpotIdRanges;
	private JComboBox<String> cbTasks;
	private JCheckBox checkboxDuplicate;
	private JCheckBox checkboxFilter;
	private String lastPath = ".";
	private Config config;
	private List<Pair<Long,Long>> userIdRanges;
	private List<Pair<Long,Long>> spotIdRanges;
	private String[] taskNames  = {"User Task", "UserFriend Task", "UserTopSpots Task", "UserVisitedSpots Task", "Spot Task", "SpotEvents Task", "BrightkiteTask"};
	private int selectedTaskIdx = 0;
	private String configPath;

	public Application(JFrame frame) {
		super();

		this.frame =  frame;
		// init config
		configPath = System.getProperty("user.home") + "/crawler/config";
		// TODO: save config file 
		// System.out.println("User Home Path: " + System.getProperty("user.home"));
		//Config oldConfig = (Config) FileUtil.recoverObjectFromFile(configPath);
		Config oldConfig = null;
		if( oldConfig == null ){
			config = new Config();
		}else{
			config = oldConfig;
		}

		this.setLayout(new GridBagLayout());
		{
			// parameters panel
			JPanel parametersPanel = new JPanel();
			parametersPanel.setLayout(new BoxLayout(parametersPanel, BoxLayout.Y_AXIS));
			textMinUserId = new JTextField("0");
			textMinUserId.setLocation(200, 0);
			textMaxUserId = new JTextField("0");
			textMaxUserId.setLocation(200, 0);
			textMinSpotId = new JTextField("0");
			textMinSpotId.setLocation(200, 0);
			textMaxSpotId = new JTextField("0");
			textMaxSpotId.setLocation(200, 0);

			cbUserIdRanges = new JComboBox<String>();
			cbSpotIdRanges = new JComboBox<String>();
			cbTasks = new JComboBox<String>();
			userIdRanges = new ArrayList<Pair<Long,Long>>();
			spotIdRanges = new ArrayList<Pair<Long,Long>>();

			JPanel panel1 = new JPanel();
			JPanel panel2 = new JPanel();
			JPanel panel3 = new JPanel();
			JPanel panel4 = new JPanel();
			JPanel panel5 = new JPanel();

			panel1.setLayout(new BoxLayout(panel1, BoxLayout.X_AXIS));
			panel2.setLayout(new BoxLayout(panel2, BoxLayout.X_AXIS));
			panel3.setLayout(new BoxLayout(panel3, BoxLayout.X_AXIS));
			panel4.setLayout(new BoxLayout(panel4, BoxLayout.X_AXIS));
			panel5.setLayout(new BoxLayout(panel5, BoxLayout.X_AXIS));


			// # add user ID ranges 
		    setUserRangeButton = new JButton(" Add Range");
			setUserRangeButton.addActionListener(this);
			panel1.add(Box.createHorizontalStrut(50));
			panel1.add(textMinUserId);
			panel1.add(textMaxUserId);
			panel1.add(setUserRangeButton);


			// User ID ranges 
			final JLabel lb2 = new JLabel(" User Id Range");
			lb2.setPreferredSize(new Dimension(150, 0));
			panel2.add(lb2);
			cbUserIdRanges.addActionListener(this);
			panel2.add(cbUserIdRanges);

			//add spot ID ranges 
			setSpotRangeButton = new JButton(" Add Range");
			setSpotRangeButton.addActionListener(this);
			panel3.add(Box.createHorizontalStrut(50));
			panel3.add(textMinSpotId);
			panel3.add(textMaxSpotId);
			panel3.add(setSpotRangeButton);

			// Spot ID ranges 
			final JLabel lb4 = new JLabel(" Spot Id Range");
			lb4.setPreferredSize(new Dimension(150, 0));
			panel4.add(lb4);
			cbSpotIdRanges.addActionListener(this);
			panel4.add(cbSpotIdRanges);

			// tasks
			final JLabel lb5 = new JLabel(" Select a Task");
			lb5.setPreferredSize(new Dimension(150, 0));
			panel5.add(lb5);
			cbTasks.addActionListener(this);
			panel5.add(cbTasks);

			parametersPanel.add(panel1);
			parametersPanel.add(panel2);
			parametersPanel.add(panel3);
			parametersPanel.add(panel4);
			parametersPanel.add(panel5);

			// Add the parameters panel to main frame.
			GridBagConstraints constraints = new GridBagConstraints();
			constraints.fill = GridBagConstraints.HORIZONTAL;
			constraints.gridx = 0;
			constraints.gridy = 0;
			constraints.weightx = 1;
			constraints.weighty = 1;
			constraints.ipadx = 10;
			add(parametersPanel, constraints);
		}

		{
			// Button panel
			JPanel buttonPanel = new JPanel();
			buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.X_AXIS));

			// button to start
			runButton1 = new JButton("Start");
			runButton1.addActionListener(this);
			buttonPanel.add(runButton1);
			buttonPanel.add(Box.createHorizontalStrut(50));

			// button to stop
			runButton2 = new JButton("Stop");
			runButton2.addActionListener(this);
			buttonPanel.add(runButton2);
			buttonPanel.add(Box.createHorizontalStrut(50));

			// button to save config to user's home directory
			runButton3 = new JButton("Save Config");
			runButton3.addActionListener(this);
			buttonPanel.add(runButton3);
			buttonPanel.add(Box.createHorizontalStrut(50));

			// button to load ids from external file 
			runButton4 = new JButton("Load Ranges");
			runButton4.addActionListener(this);
			buttonPanel.add(runButton4);
			buttonPanel.add(Box.createHorizontalStrut(50));

			// button to set log level 
			runButton5 = new JButton("Clean Log");
			runButton5.addActionListener(this);
			buttonPanel.add(runButton5);
			buttonPanel.add(Box.createHorizontalStrut(50));

			// check duplicate
			checkboxDuplicate = new JCheckBox("Check Dup");
			checkboxDuplicate.addActionListener(this);
			buttonPanel.add(checkboxDuplicate);
			buttonPanel.add(Box.createHorizontalStrut(50));
			
			// check if need to filter cities for brightkite
			checkboxFilter = new JCheckBox("Filter cities", true);
			checkboxFilter.addActionListener(this);
			
			buttonPanel.add(checkboxFilter);
			buttonPanel.add(Box.createHorizontalStrut(50));
						
			// save queue button
			saveTaskQueueButton = new JButton("Save queue");
			saveTaskQueueButton.addActionListener(this);
			buttonPanel.add(saveTaskQueueButton);
			buttonPanel.add(Box.createHorizontalStrut(50));
						
			// about button
			aboutButton = new JButton("About");
			aboutButton.addActionListener(this);
			buttonPanel.add(aboutButton);
			buttonPanel.add(Box.createHorizontalStrut(50));
		
			// add start/stop buttons to panel
			buttonPanel.add(Box.createHorizontalStrut(50));
			GridBagConstraints constraints = new GridBagConstraints();
			constraints.fill = GridBagConstraints.HORIZONTAL;
			constraints.gridx = 0;
			constraints.gridy = 1;
			constraints.weightx = 1.0;
			constraints.weighty = 0.01;
			constraints.ipady = 20;
			add(buttonPanel, constraints);
		}

		{
			// setup status ouput area
			statusLogPanel = new GuiLogPanel(800, 200, false);
			//statusLogPanel.setPreferredSize(new Dimension(800, 200));
			// Add the output pane to the bottom
			GridBagConstraints constraints = new GridBagConstraints();
			constraints.fill = GridBagConstraints.BOTH;
			constraints.gridx = 0;
			constraints.gridy = 2;
			constraints.weightx = 1;
			constraints.weighty = 1;
			add(statusLogPanel, constraints);
		}
		{   
			// setup text output area
			taskLogPanel = new GuiLogPanel(800, 400, true);
			// Add the output pane to the bottom
			GridBagConstraints constraints = new GridBagConstraints();
			constraints.fill = GridBagConstraints.BOTH;
			constraints.gridx = 0;
			constraints.gridy = 3;
			constraints.weightx = 1;
			constraints.weighty = 1;
			add(taskLogPanel, constraints);
		}

		// init combobox selection
		for(String task : taskNames){
			cbTasks.addItem( task );
		}
		
		if( oldConfig != null ){
			statusLogPanel.log("Loaded configuration from : " + configPath);
		}
	}
	
	/**
	 * Action handler for events
	 */
	public void actionPerformed(ActionEvent e) {
		Object src = e.getSource();
		
		if( src.equals( runButton5 ) ){
			int confirm = JOptionPane.showConfirmDialog(null,
					"Do you want to make the log clearer ?", "Hey!",
					JOptionPane.YES_NO_OPTION);
			if( confirm == JOptionPane.YES_OPTION ){
				LoggerFactory.setLogClean();
				statusLogPanel.log( "Log level has been changed to info.");
			}
		}else if( src.equals(runButton4)){
			JFileChooser jfc = new JFileChooser();
			jfc.setCurrentDirectory(new File(lastPath));
			
			if(jfc.showOpenDialog(Application.this) == JFileChooser.APPROVE_OPTION ){
				String filePath = jfc.getSelectedFile().getAbsolutePath();
		        lastPath = filePath.substring(0, filePath.lastIndexOf(File.separator));
		        
				config.setRangeFilePath(filePath);
				config.setRangeFromFile(true);
				statusLogPanel.log( "You have chosen to load ranges from file: " + filePath);
			}
		}else if( src.equals(runButton3)){
			Thread r = new Thread() {
				public void run() {
					// save file 
					FileUtil.saveObjectToFile(config, configPath);
					statusLogPanel.log("Configuration saved to : " + configPath);
				}
			};
			r.start();
		}else if( src.equals(runButton2)){
			Thread r = new Thread() {
				public void run() {
					stopTasks();
				}
			};
			r.start();
		}else if( src.equals(runButton1)){
			Thread r = new Thread() {
				public void run() {
					runTasks();
				}
			};
			r.start();
		}else if( src.equals(cbTasks)){
			int selectedId = cbTasks.getSelectedIndex();
			if( selectedId >= 0 && selectedId < taskNames.length ){
				selectedTaskIdx = selectedId;
				statusLogPanel.log("You have chosen task : " + selectedId);
			}
		}else if( src.equals(cbSpotIdRanges)){
			int selectedId = cbSpotIdRanges.getSelectedIndex();
			if( selectedId >= 0 && selectedId < spotIdRanges.size() ){
				config.setSpotIdRange( spotIdRanges.get(selectedId));
				//logPanel.log("You have chosen : " + selectedId);
			}
		}
		else if(src.equals(cbUserIdRanges)){
			int selectedId = cbUserIdRanges.getSelectedIndex();
			if( selectedId >= 0 && selectedId < userIdRanges.size() ){
				config.setUserIdRange( userIdRanges.get(selectedId));
				//logPanel.log("You have chosen : " + selectedId);
			}
		}
		else if( src.equals(setSpotRangeButton)){
			String strMin = textMinSpotId.getText();
			String strMax = textMaxSpotId.getText();
			long min = 0;
			long max = 0;
			try{
				min = Long.valueOf(strMin);
				max = Long.valueOf(strMax);
			}catch(Exception ne){}

			if(  min > 0 && max > 0 && min <= max){
				cbSpotIdRanges.addItem( min + " - " + max );
				spotIdRanges.add(new Pair<Long,Long>(min, max)); 
				cbSpotIdRanges.setSelectedIndex( spotIdRanges.size() -1 );
				statusLogPanel.log( "Range [" +  min + " - " + max +"] is added to the list " );
			}else{
				statusLogPanel.log( "The min/max range is not valid .");
			}  
		}else if( src.equals(setUserRangeButton)){
			String strMin = textMinUserId.getText();
			String strMax = textMaxUserId.getText();
			long min = 0;
			long max = 0;
			try{
				min = Long.valueOf(strMin);
				max = Long.valueOf(strMax);
			}catch(Exception ne){}

			if(  min > 0 && max > 0 && min <= max){
				cbUserIdRanges.addItem( min + " - " + max );
				userIdRanges.add(new Pair<Long,Long>(min, max)); 
				// set selected
				cbUserIdRanges.setSelectedIndex( userIdRanges.size() -1 );
				statusLogPanel.log( "Range [" +  min + " - " + max +"] is added to the list " );
			}else{
				statusLogPanel.log( "The min/max range is not valid .");
			}   
		}
		else if( src.equals(checkboxFilter)){
			if( checkboxFilter.isSelected() ){
				statusLogPanel.log( "The brightkite task will be filtered in following cities: \n" + BrightkiteTask.TOP_CITIES );
				config.setFilterCities(true);
			}else{
				statusLogPanel.log( "The brightkite task will not be filtered by cities."  );
				config.setFilterCities(false);
			}
		}
		else if( src.equals(checkboxDuplicate)){
			if( checkboxDuplicate.isSelected() ){
				statusLogPanel.log( "Check duplicate IDs."  );
				config.setCheckDuplicateId(true);
			}else{
				statusLogPanel.log( "Don't check duplicate IDs."  );
				config.setCheckDuplicateId(false);
			}
		}
		else if( src.equals(saveTaskQueueButton)){
			TaskEngine taskEngine = TaskEngine.getInstance();
			Task task = taskEngine.getTask( BrightkiteTask.NAME );
			if( task == null ){
				statusLogPanel.log( "Task not found: " + BrightkiteTask.NAME);
				return;
			}

	        // get string representation of queques
	        BrightkiteTask btask = (BrightkiteTask)task;
	        
	        BlockingQueue<String> personQueue = btask.getPersonQueue();
	        BlockingQueue<String> placeQueue = btask.getPlaceQueue();
	        
	        statusLogPanel.log( "Queue information" + ", person ids:" + personQueue.size() + ", place ids:" + placeQueue.size() + ", time: " + GuiUtil.getDateAllInfo(System.currentTimeMillis())  );
	        
	        // confirm if need to save
	        int confirm = JOptionPane.showConfirmDialog(this,
					"Do you want to save the queue information ?", "Hey!",
					JOptionPane.YES_NO_OPTION);
			if( confirm != JOptionPane.YES_OPTION ){
				return;
			}
	        
	        StringBuilder sb1 = new StringBuilder();
	        for(String id : personQueue){
	        	sb1.append(id).append("\n");
	        }
	        StringBuilder sb2 = new StringBuilder();
	        for(String id : placeQueue){
	        	sb2.append(id).append("\n");
	        }
	        
	        // select a dir
	        JFileChooser chooser = new JFileChooser();
	        chooser.setCurrentDirectory(new File(lastPath));
	        chooser.setFileSelectionMode(1);// dir only
	        int returnVal = chooser.showOpenDialog( this );
	        if(returnVal != JFileChooser.APPROVE_OPTION ){
	        	return;
	        }

	        // remember last path
	        String outDir = chooser.getSelectedFile().getPath();
	        lastPath = outDir.substring(0, outDir.lastIndexOf(File.separator));
	     	        
	        File dir = new File(outDir);
	        if(!dir.exists()){
	        	confirm = JOptionPane.showConfirmDialog(this,
	    				"Directory not exists, do you want to create it ?", "Hey!",
	    				JOptionPane.YES_NO_OPTION);
	    		if( confirm == JOptionPane.YES_OPTION ){
	    			//create dir
	    			boolean ret = dir.mkdir();
	    			statusLogPanel.log("create dir, " + dir + "," + ret);
	    		}
	    		else{
	    			return;
	    		}
	        }
	        
	        File file1 = new File( outDir + File.separator + BrightkiteTask.PERSON_QUEUE_FILE);
	        File file2 = new File( outDir + File.separator + BrightkiteTask.PLACE_QUEUE_FILE);
	        
	        // delete if exist
	        if( file1.exists() ){
	        	file1.delete();
	        }
	        if( file2.exists() ){
	        	file2.delete();
	        }
	        
	        // save queques to disk
	        FileUtil.saveContentInFile(sb1.toString(), file1.getAbsolutePath(), false);
	        FileUtil.saveContentInFile(sb2.toString(), file2.getAbsolutePath(), false);
	        statusLogPanel.log( "Queue information save to dir: " + outDir + ", person ids:" + personQueue.size() + ", place ids:" + placeQueue.size() );
			
		}
		else if( src.equals(aboutButton)){
			JOptionPane.showMessageDialog(null,
					ABOUT_MSG, "About",
					JOptionPane.PLAIN_MESSAGE);
		}
	}
	
	/**
	 * init the selection list for userId/spotId ranges
	 * @param len
	 * @param minValue
	 * @param maxValue
	 * @param comboList
	 * @param idRanges
	 */
	private void initSelectionList(int len, long minValue, long maxValue, JComboBox<String> comboList, List<Pair<Long,Long>> idRanges) {
		long interval = (long) Math.floor( (maxValue - minValue)/ len );

		idRanges.clear();
		comboList.removeAllItems();

		long min = minValue;
		long max = minValue;
		for(int i = 0; i < len; i++){
			max = Math.min( min + interval, maxValue );
			String range = min + " - " + max;
			// add to list
			comboList.addItem(range);
			idRanges.add( new Pair<Long,Long>(min, max) );
			min = max + 1;
		}
	}
	
	

	/**
	 * Do a full run of the tasks according to parameters.
	 */
	protected void runTasks() {
		runButton1.setEnabled(false);
		runButton2.setEnabled(false);

		// get parameters
		config.setLogPanel(taskLogPanel);
		config.setStatusPanel(statusLogPanel);
		if( checkboxFilter.isSelected() ){
			config.setFilterCities(true);
		}else{
			config.setFilterCities(false);
		}
		
		int selectedId = cbUserIdRanges.getSelectedIndex();
		if( selectedId >= 0 && selectedId < userIdRanges.size() ){
			config.setUserIdRange( userIdRanges.get(selectedId));
		}
		selectedId = cbSpotIdRanges.getSelectedIndex();
		if( selectedId >= 0 && selectedId < spotIdRanges.size() ){
			config.setSpotIdRange( spotIdRanges.get(selectedId));
		}
		selectedId = cbTasks.getSelectedIndex();
		if( selectedId >= 0 && selectedId < taskNames.length ){
			selectedTaskIdx = selectedId;
		}

		config.setCheckDuplicateId( checkboxDuplicate.isSelected() );
		//logPanel.clear();
		statusLogPanel.log("Start running Tasks..."  );

		TaskEngine taskEngine = TaskEngine.getInstance();
		Task task = null;	   

		try {
			// Holds the task to run.
			if( selectedTaskIdx == 0 ){
				task = new UserTask();
			}else if(selectedTaskIdx == 1){
				task = new UserFriendTask();
				config.setCheckDuplicateId(false);
			}else if(selectedTaskIdx == 2){
				task = new UserTopSpotsTask();
				config.setCheckDuplicateId(false);
			}else if(selectedTaskIdx == 3){
				task = new UserVisitedSpotsTask();
				config.setCheckDuplicateId(false);
			}
			else if(selectedTaskIdx == 4){
				task = new SpotTask();
			}else if(selectedTaskIdx == 5){
				task = new SpotEventsTask();
			}else if(selectedTaskIdx == 6){
				task = new BrightkiteTask();
			}
			
			statusLogPanel.log( "Task " + task.getName()  + " is added to engine");

			statusLogPanel.log( "User Range from [" + config.getUserIdRange().first + "] to [" + config.getUserIdRange().second + "]");
			statusLogPanel.log( "Spot Range from [" + config.getSpotIdRange().first + "] to [" + config.getSpotIdRange().second + "]");
			statusLogPanel.log( "Check duplicate IDs: " + checkboxDuplicate.isSelected());

			long start = System.currentTimeMillis();

			// Add the task 
			taskEngine.addTask(task, config);

			taskEngine.runTask( task.getName() );
			
			long end = System.currentTimeMillis();
			long elapsedTime = end - start;

			//logPanel.log("The runtime is: " + elapsedTime + " ms.\n");
		}
		catch(Exception e) {
			e.printStackTrace();
		}

		runButton1.setEnabled(false);
		runButton2.setEnabled(true);
	}

	protected void stopTasks(){
		runButton1.setEnabled(false);
		runButton2.setEnabled(false);

		TaskEngine taskEngine = TaskEngine.getInstance();
		taskEngine.stopAll();

		statusLogPanel.log("Stop all tasks");

		runButton1.setEnabled(true);
		runButton2.setEnabled(false);
	}

	private void exit(){
		// need to stop all tasks first
		TaskEngine engine = TaskEngine.getInstance();
		try{
			engine.stopAll();
		}catch(Exception e){}
		//frame.dispose();
		// should stop all background threads too, so use System.exit
		System.exit(0);
	}

	/**
	 * Create the GUI and show it. For thread safety, this method should be
	 * invoked from the event-dispatching thread.
	 */
	protected static void createAndShowGUI() {
		// Create and set up the window.
		JFrame frame = new JFrame("Yet Another Crawler! ");
		// frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		//Set the default close operation so the window won't close
		frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE); 

		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		}
		catch(Exception e) {
			// ignore
		}


		// adjust the frame window to the center of screen
		Toolkit tk = Toolkit.getDefaultToolkit();
		Dimension screenSize = tk.getScreenSize();
		int screenHeight = screenSize.height;
		int screenWidth = screenSize.width;
		frame.setSize(screenWidth / 2, screenHeight / 2);
		frame.setLocation(screenWidth / 4, screenHeight / 4);

		// Create and set up the content pane.
		Application newContentPane = new Application( frame );
		newContentPane.setOpaque(true);
		frame.setContentPane(newContentPane);
		// Display the window.
		frame.pack();
		frame.setVisible(true);
		// set close listener
		frame.addWindowListener( newContentPane );
	}

	/**
	 * Main method that just spawns the UI.
	 * 
	 * @param args command line parameters
	 */
	public static void main(String[] args) {
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
	}

	@Override
	public void windowOpened(WindowEvent e) {	
	}

	@Override
	public void windowClosing(WindowEvent e) {
		int confirm = JOptionPane.showConfirmDialog(this,
				"Are you sure you want to quit ?", "Hey!",
				JOptionPane.YES_NO_OPTION);
		if( confirm == JOptionPane.YES_OPTION ){
			//Close frame
			exit();
		}
	}

	@Override
	public void windowClosed(WindowEvent e) {	
	}

	@Override
	public void windowIconified(WindowEvent e) {	
	}

	@Override
	public void windowDeiconified(WindowEvent e) {	
	}

	@Override
	public void windowActivated(WindowEvent e) {	
	}

	@Override
	public void windowDeactivated(WindowEvent e) {	
	}
}