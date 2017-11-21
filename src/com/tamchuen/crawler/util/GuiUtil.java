package com.tamchuen.crawler.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 
 * @author  Dequan
 * Project: Crawler
 * Date:    Dec 7, 2011
 * 
 */
public class GuiUtil {
	/**
	 * get string representation of date time eg: 2006-02-21 17:49:43.156
	 * 
	 * @param time
	 * @return
	 */
	public static final String getDateAllInfo(long time) {
		Date date = new Date(time);
		Calendar ca = Calendar.getInstance();
		ca.setTime(date);
		int month = 1 + ca.get(Calendar.MONTH);
		String monthStr = String.valueOf(month);
		if (month < 10) {
			monthStr = "0" + monthStr;
		}
		int day = ca.get(Calendar.DAY_OF_MONTH);
		String dayStr = String.valueOf(day);
		if (day < 10) {
			dayStr = "0" + dayStr;
		}
		int hour = ca.get(Calendar.HOUR_OF_DAY);
		String hourStr = String.valueOf(hour);
		if (hour < 10) {
			hourStr = "0" + hourStr;
		}
		int minute = ca.get(Calendar.MINUTE);
		String minuteStr = String.valueOf(minute);
		if (minute < 10) {
			minuteStr = "0" + minuteStr;
		}
		int second = ca.get(Calendar.SECOND);
		String secondStr = String.valueOf(second);
		if (second < 10) {
			secondStr = "0" + second;
		}

		StringBuilder strBuf = new StringBuilder();
		strBuf.append(ca.get(Calendar.YEAR)).append("-").append(monthStr).append("-").append(dayStr);
		strBuf.append(" ").append(hourStr).append(":").append(minuteStr).append(":").append(secondStr).append(".").append(ca.get(Calendar.MILLISECOND));
		return strBuf.toString();
	}
}
