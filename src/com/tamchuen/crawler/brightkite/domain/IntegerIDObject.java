package com.tamchuen.crawler.brightkite.domain;

/**
 * 
 * @author  Dequan
 * Project: Crawler
 * Date:    Dec 1, 2011
 * 
 */
public abstract class IntegerIDObject {
	private long id;

	public IntegerIDObject(){
		this.id = -1L;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
}
