package com.tamchuen.crawler.ui;

import java.awt.Dimension;
import javax.swing.BoxLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 * Panel that contains a text logging pane 
 * @author Dequan
 */
public class GuiLogPanel extends JPanel {
	/**
	 * Serial
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The actual text logging panel, will have a scroll bar TODO: auto scroll
	 * 
	 * 
	 */
	protected GuiLogPane logpane = new GuiLogPane();

	public GuiLogPanel() {
		this(200, 200, false);
	}

	public GuiLogPanel(int width, int height, boolean needAutoClean) {
		super();
		logpane = new GuiLogPane(needAutoClean);
		super.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
		JScrollPane outputPane = new JScrollPane(logpane);
		outputPane.setPreferredSize(new Dimension(width, height));
		super.add(outputPane);
	}

	/**
	 * Clear the current contents.
	 */
	public void clear() {
		logpane.clear();
	}

	/**
	 * Log a msg into the log panel.
	 * 
	 * @param msg 
	 */
	public void log(String msg) {
		try {
			logpane.log(msg);
		}
		catch(Exception e) {
			throw new RuntimeException("Error logging a message.", e);
		}
	}
}