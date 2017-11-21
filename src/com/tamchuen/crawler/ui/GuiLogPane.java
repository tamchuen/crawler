package com.tamchuen.crawler.ui;

import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JTextPane;
import javax.swing.text.Style;
/**
 * a text pane for logging output.
 * @author Dequan
 */
public class GuiLogPane extends JTextPane {
	/**
	 * Serialization version number
	 */
	private static final long serialVersionUID = 1L;

	private Style baseStyle;

	private boolean needAutoClean;

	private AtomicInteger integer;
	/**
	 * Constructor
	 */
	public GuiLogPane() {
		this(false);
	}
	/**
	 * Constructor
	 */
	public GuiLogPane(boolean needAutoClean) {
		super();
		this.needAutoClean = needAutoClean;
		integer = new AtomicInteger(0);
		baseStyle = getStyledDocument().addStyle(null, null);
	}
	/**
	 * Print a message into the text pane
	 * 
	 * @param message Message text
	 */
	public void log(String message) {
		try {
			getStyledDocument().insertString(getStyledDocument().getLength(), message, baseStyle);
			getStyledDocument().insertString(getStyledDocument().getLength(),"\n",baseStyle);
			int line = integer.addAndGet(1);
			if( needAutoClean && line % 100 ==0){
				clear();
			}
		}
		catch(Exception e) {
			throw new RuntimeException("Error writing a message.", e);
		}
	}
	/**
	 * Clear the current contents.
	 */
	public void clear() {
		setText("");
	}
}
