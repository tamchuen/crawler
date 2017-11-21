package com.tamchuen.crawler;
import java.io.Serializable;

import com.tamchuen.crawler.ui.GuiLogPanel;
import com.tamchuen.jutil.util.Pair;


/**
 * the configuration object
 * @author Dequan
 *
 */
public class Config implements Serializable{

    private GuiLogPanel logPanel;
    private GuiLogPanel statusPanel;

    private Pair<Long,Long> userIdRange;
    private Pair<Long,Long> spotIdRange;
    private Pair<Long,Long> checkinIdRange;
    
    private boolean isFilterCities;
    private boolean isRangeFromFile;
    /**
     * if it's in update mode, then if there is an already existed record, will try to update it.
     * Otherwise will ignore this record ( no insert ) 
     */
    private boolean isUpdateMode;
    private String rangeFilePath;
    /**
     * in the initialization phase, if the ID already exists in DB, then remove it from range
     */
    private boolean isCheckDuplicateId;
    
    public Config(){
	userIdRange = new Pair<Long,Long>(-1L, -1L);
	spotIdRange = new Pair<Long,Long>(-1L, -1L);
	checkinIdRange = new Pair<Long,Long>(-1L, -1L);
	isRangeFromFile = false;
	isUpdateMode = true;
    }
    public GuiLogPanel getLogPanel() {
        return logPanel;
    }
    public void setLogPanel(GuiLogPanel logPanel) {
        this.logPanel = logPanel;
    }
    public Pair<Long, Long> getUserIdRange() {
        return userIdRange;
    }
    public void setUserIdRange(Pair<Long, Long> userIdRange) {
        this.userIdRange = userIdRange;
    }
    public Pair<Long, Long> getSpotIdRange() {
        return spotIdRange;
    }
    public void setSpotIdRange(Pair<Long, Long> spotIdRange) {
        this.spotIdRange = spotIdRange;
    }
    public Pair<Long, Long> getCheckinIdRange() {
        return checkinIdRange;
    }
    public void setCheckinIdRange(Pair<Long, Long> checkinIdRange) {
        this.checkinIdRange = checkinIdRange;
    }

    public GuiLogPanel getStatusPanel() {
        return statusPanel;
    }
    public void setStatusPanel(GuiLogPanel statusPanel) {
        this.statusPanel = statusPanel;
    }
    public boolean isRangeFromFile() {
        return isRangeFromFile;
    }
    public void setRangeFromFile(boolean isRangeFromFile) {
        this.isRangeFromFile = isRangeFromFile;
    }
    public String getRangeFilePath() {
        return rangeFilePath;
    }
    public void setRangeFilePath(String rangeFilePath) {
        this.rangeFilePath = rangeFilePath;
    }
    
    public boolean isCheckDuplicateId() {
        return isCheckDuplicateId;
    }
    public void setCheckDuplicateId(boolean isCheckDuplicateId) {
        this.isCheckDuplicateId = isCheckDuplicateId;
    }
    public boolean isUpdateMode() {
        return isUpdateMode;
    }
    public void setUpdateMode(boolean isUpdateMode) {
        this.isUpdateMode = isUpdateMode;
    }
	public boolean isFilterCities() {
		return isFilterCities;
	}
	public void setFilterCities(boolean isFilterCities) {
		this.isFilterCities = isFilterCities;
	}
    
}
