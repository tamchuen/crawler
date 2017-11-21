package com.tamchuen.crawler.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

/**
 * Util class for doing HTTP GET/POST requests and return as string
 * @author Dequan
 *
 */
public class HttpUtil
{
    //private static Logger logger = Logger.getLogger(Constants.SYS_LOG_NAME);
    
    private static  int defaultConnectTimeout = 3000;
    private static  int defaultReadTimeOut = 3000;
    private static  String defaultResponseCharset = "utf-8";
    private static  String defaultRequestCharset = "utf-8";
    
    /**
     * 以post方式请求某个URL，返回请求得到的内容
     * @param URL
     * @return
     * @throws Exception
     */
    public static String requestByPost(String URL )
            throws Exception {
        return requestByPost(URL, null, defaultConnectTimeout,  defaultReadTimeOut,  defaultResponseCharset, defaultRequestCharset );
    }
    /**
     * 以post方式请求某个URL，返回请求得到的内容
     * @param URL
     * @param parameters
     * @return
     * @throws Exception
     */
    public static String requestByPost(String URL, Map<String, String> parameters )
            throws Exception {
        return requestByPost(URL, parameters, defaultConnectTimeout,  defaultReadTimeOut,  defaultResponseCharset, defaultResponseCharset );
        
    }
    /**
     * 以post方式请求某个URL，返回请求得到的内容
     * @param URL 
     * @param parameters 参数
     * @param connectTimeout 请求时连接超时时间
     * @param readTimeOut   读数据时超时时间
     * @param responseCharset 返回的字符集类型
     * @return  String
     * @throws Exception
     */
    public static String requestByPost(String URL, Map<String, String> parameters, int connectTimeout, int readTimeOut,  String responseCharset, String requestCharset )
            throws Exception {
        URL url;
        HttpURLConnection uc = null;
        InputStream in = null;
        try {
            url = new URL(URL);
            uc = (HttpURLConnection) url.openConnection();
            // 设立sockettimeout时间
            uc.setConnectTimeout(connectTimeout);
            uc.setReadTimeout(readTimeOut);
            uc.setRequestMethod("POST");
            uc.setDoOutput(true);
            uc.setDoInput(true);
            OutputStreamWriter wr = new OutputStreamWriter(
                    uc.getOutputStream(),responseCharset);
            String postString = populatePostString(parameters,requestCharset);
            if ( null != postString && !"".equals(postString) )
            {
                wr.write(postString);
            }
            wr.flush();

            in = uc.getInputStream();
            
            BufferedReader breader = new BufferedReader(new InputStreamReader(
                    in, responseCharset));

            StringBuffer sbContent = new StringBuffer();
            String tmp = "";
            while ((tmp = breader.readLine()) != null) {
                sbContent.append(tmp + "\n");
            }
            return sbContent.toString();
        } catch (Exception e) {
            throw e;
        } finally {
            if (in != null) {
                try
                {
                    in.close();
                } catch (IOException e)
                {
                }
            }
            if (uc != null) {
                uc.disconnect();
            }
        }
    }

    /**
     * 以post方式请求某个URL，返回请求得到的内容
     * @param URL 
     * @param postString 参数
     * @param connectTimeout 请求时连接超时时间
     * @param readTimeOut   读数据时超时时间
     * @param responseCharset 返回的字符集类型
     * @return  String
     * @throws Exception
     */
    public static String requestByPostWithStr(String URL, String requestContentType,String postString, int connectTimeout, int readTimeOut,  String responseCharset, String requestCharset )
            throws Exception {
        URL url;
        HttpURLConnection uc = null;
        InputStream in = null;
        try {
            url = new URL(URL);
            uc = (HttpURLConnection) url.openConnection();
            // 设立sockettimeout时间
            uc.setConnectTimeout(connectTimeout);
            uc.setReadTimeout(readTimeOut);
            uc.setRequestMethod("POST");
            uc.setRequestProperty("Content-Type",requestContentType);
            uc.setDoOutput(true);
            uc.setDoInput(true);
            OutputStreamWriter wr = new OutputStreamWriter(
                    uc.getOutputStream(),responseCharset);
            if ( null != postString && !"".equals(postString) )
            {
                wr.write(postString);
            }
            wr.flush();

            in = uc.getInputStream();
            
            BufferedReader breader = new BufferedReader(new InputStreamReader(
                    in, responseCharset));

            StringBuffer sbContent = new StringBuffer();
            String tmp = "";
            while ((tmp = breader.readLine()) != null) {
                sbContent.append(tmp + "\n");
            }
            return sbContent.toString();
        } catch (Exception e) {
            throw e;
        } finally {
            if (in != null) {
                try
                {
                    in.close();
                } catch (IOException e)
                {
                }
            }
            if (uc != null) {
                uc.disconnect();
            }
        }
    }
    
    public static String requestByGet(String URL )throws Exception {
        return requestByGet(URL, null, null, defaultConnectTimeout, defaultReadTimeOut,  defaultResponseCharset, defaultRequestCharset );
    }
    public static String requestByGet(String URL, int timeout )
            throws Exception {
        return requestByGet(URL, null, null, timeout,  timeout,  defaultResponseCharset, defaultRequestCharset );
    }
    public static String requestByGet(String URL, Map<String, String> parameters, Map<String, String> headers)
        throws Exception {
        return requestByGet(URL,  parameters, headers, defaultConnectTimeout, defaultReadTimeOut,  defaultResponseCharset, defaultRequestCharset );
    }
    
    /**
     * 以get方式请求某个URL，返回请求得到的内容
     * @param URL
     * @param parameters 参数
     * @param headers 参数
     * @param connectTimeout 请求时连接超时时间
     * @param readTimeOut 读数据时超时时间
     * @param responseCharset 返回的字符集类型
     * @param URLCharset URL参数需要ecode的字符集，如"utf-8"
     * @return String
     * @throws Exception
     */
    public static String requestByGet(String URL, Map<String, String> parameters,Map<String, String> headers, int connectTimeout, int readTimeOut,  String responseCharset, String URLCharset )
            throws Exception {
        URL url;
        HttpURLConnection uc = null;
        InputStream in = null;
        try {
            // set http url 
            String getString = populateGetString( parameters, URLCharset);
            if ( getString != null && !"".equals(getString) )
            {
                if ( URL.indexOf("?") >= 0 )
                {
                    url = new URL(URL + "&" + getString);
                }
                else 
                {
                    url = new URL(URL + "?" + getString);
                }
            }
            else 
            {
                url = new URL( URL );
            }
            uc = (HttpURLConnection) url.openConnection();
            // 设立sockettimeout时间
            uc.setConnectTimeout(connectTimeout);
            uc.setReadTimeout(readTimeOut);
            uc.setRequestMethod("GET");
            // set HTTP headers
            if(headers!= null && headers.size() >0  ){
        	for (Iterator< Map.Entry<String, String>> i = headers.entrySet().iterator(); i.hasNext(); ) {
                    Map.Entry<String, String> e = i.next();
                    uc.addRequestProperty( e.getKey(), e.getValue() );
                }
            }
            uc.connect();
            in = uc.getInputStream();
            BufferedReader breader = new BufferedReader(new InputStreamReader(
                    in, responseCharset));
            
            StringBuffer sbContent = new StringBuffer();
            String tmp = "";
            while ((tmp = breader.readLine()) != null) {
                sbContent.append(tmp + "\n");
            }
            return sbContent.toString();
        } catch (Exception e) {
            throw e;
        } finally {
            if (in != null) {
                try
                {
                    in.close();
                } catch (IOException e)
                {
                }
            }
            if (uc != null) {
                uc.disconnect();
            }
        }
    }
    
    /**
     * 构造请求数据
     * @param parameters 要请求的参数名和参数值对
     * @return String 返回请求得到的字符串
     */
    private static String populatePostString(Map<String, String> parameters,String charSet)
    {
        if ( null == parameters || parameters.size() == 0 )
        {
            return "";
        }
        StringBuffer aReturn = new StringBuffer();
        for (Iterator< Map.Entry<String, String>> i = parameters.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, String> e = i.next();
            try
            {
                appendPostString(e.getKey(), new String(e.getValue().getBytes("GBK"),charSet), aReturn, "&");
            } catch (UnsupportedEncodingException e1)
            {
                e1.printStackTrace();
            }
        }
        return aReturn.toString();
    }

    private static String populateGetString(Map<String, String> parameters, String URLCharset)
    {
        if ( null == parameters || parameters.size() == 0 )
        {
            return "";
        }
        StringBuffer aReturn = new StringBuffer("");
        for (Iterator< Map.Entry<String, String>> i = parameters.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, String> e = i.next();
            appendQueryString(e.getKey(), e.getValue(), aReturn, "&", URLCharset );
        }
        return aReturn.toString();
    }


    private static StringBuffer appendPostString(String key, String value,
        StringBuffer postString, String ampersand) {
        if (postString.length() > 0) {
            postString.append(ampersand);
        }
        postString.append(key);
        postString.append("=");
        postString.append(value);
        return postString;
    }
    
    private static StringBuffer appendQueryString(String key, String value,
        StringBuffer queryString, String ampersand, String URLCharset) {
        if ( queryString.length() > 0 ) {
            queryString.append(ampersand);
        }
        try {
            queryString.append(URLEncoder.encode(key, URLCharset));
            queryString.append("=");
            queryString.append(URLEncoder.encode(value, URLCharset));
        } catch (UnsupportedEncodingException e) {
            try
            {//直接用默认的UTF-8
                queryString.append(URLEncoder.encode(key, "UTF-8"));
                queryString.append("=");
                queryString.append(URLEncoder.encode(value, "UTF-8"));
            } catch (UnsupportedEncodingException e1)
            {
            }
        }
        return queryString;
    }
    
    
    public static void main(String[] args) 
    {
        try
        {
           
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

