package com.turing.java.flink20.pipeline;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import com.etl.utls.IpParser;

/**
 * Apache服务器日志Log解析
 * https://blog.csdn.net/weixin_39469127/article/details/90419026
 * */
public class LogParser {

    //Apache服务器日志信息的正则表达式
    private static final String APACHE_LOG_REGEX =
            "^([0-9.]+)\\s([\\w.-]+)\\s([\\w.-]+)\\s\\[([^\\[\\]]+)\\]\\s\"((?:[^"
                    + "\"]|\\\")+)\"\\s(\\d{3})\\s(\\d+|-)\\s\"((?:[^\"]|\\\")+)\"\\s\"((?:"
                    + "[^\"]|\\\")+)\"\\s\"(.+)\"\\s(\\d+|-)\\s(\\d+|-)\\s(\\d+|-)\\s(.+)\\"
                    + "s(\\d+|-)$";

    //用户浏览器信息的正则表达式
    private static final String USER_AGENT_REGEX = "^(.+)\\s\\((.+)\\)\\s(.+)\\s\\((.+)\\)\\s(.+)\\s(.+)$";

    //测试日志一条
    public static final String LOG = "120.196.145.58 "
            + "- "
            + "- "
            + "[11/Dec/2013:10:00:32 +0800] "
            + "\"GET /__utm.gif HTTP/1.1\" "
            + "200 "
            + "35 "
            + "\"http://easternmiles.ceair.com/flight/index.html\" "
            + "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
            + "Chrome/31.0.1650.63 Safari/537.36\" "
            + "\"BIGipServermu_122.119.122.14=192575354.20480.0000;Webtrends=120.196.145.58.1386724976245806;"
            + "uuid=5eb501f9-3586-4239-a15c-fe89aa14d624;userId=099;st=1\" "
            + "1482 "
            + "352 "
            + "- "
            + "easternmiles.ceair.com "
            + "794";

    public static final String CANNOT_GET = "can not get";

    //需要解析的字段
    private String ipAddress = null; //ip地址
    private String uniqueId = null; //uuid
    private String url = null; //访问的url地址
    private String sessionId = null; //会话id
    private String sessionTimes = null; //服务器响应时间
    private String areaAddress = null; //国家
    private String localAddress = null; //地区
    private String browserType = null; //浏览器类型
    private String operationSys = null; //操作系统
    private String referUrl = null; //访问的上一个页面url
    private String receiveTime = null; //服务器接收请求时间
    private String userId = null; //用户id

    //保存cookies信息的map
    private static Map<String, String> cookies = new HashMap<String, String>();

    //实例化的LogParser计数
    private static int count = 0;
    //LogParser唯一标识
    private int index = 0;
    private LogParser() {
        count++;
        index = count;
    }

    //初始化Log解析器
    private void init() {
        setIpAddress(CANNOT_GET);
        setUniqueId(CANNOT_GET);
        setUrl(CANNOT_GET);
        setSessionId(CANNOT_GET);
        setSessionTimes(CANNOT_GET);
        setAreaAddress(CANNOT_GET);
        setLocalAddress(CANNOT_GET);
        setBrowserType(CANNOT_GET);
        setOperationSys(CANNOT_GET);
        setReferUrl(CANNOT_GET);
        setReceiveTime(CANNOT_GET);
        setUserId(CANNOT_GET);
        cookies.clear();
    }

    /*
     * 解析一条log日志
     *
     * @param log 一条日志
     */
    public void parse(String log) {
        //初始化Log解析器
        init();

        String ipStr = null;
        String receiveTimeStr = null;
        String urlStr = null;
        String referUrlStr = null;
        String userAgentStr = null;
        String cookieStr = null;
        String hostNameStr = null;

        //正则解析日志
        Pattern pattern = Pattern.compile(APACHE_LOG_REGEX);
        Matcher matcher = pattern.matcher(LOG);
        if (matcher.find()) {
//			for (int i = 0; i <= matcher.groupCount(); i++) {
//				System.out.println("group-" + i + " : " + matcher.group(i));
//			}

            //根据正则表达式将日志文件断开
            ipStr = matcher.group(1); //远端主机（访客IP地址）
            receiveTimeStr = matcher.group(4); //服务器接收请求时间
            urlStr = matcher.group(5); //请求的第一行（请求方式、请求的URL、请求所用协议）
            referUrlStr = matcher.group(8); //上一个访问的页面（访客来源）
            userAgentStr = matcher.group(9); //访客浏览器信息
            cookieStr = matcher.group(10); //Cookie信息（包含uuid/userId/sessiontime）
            hostNameStr = matcher.group(14); //访问主机地址

            //保存IP地址
            ipAddress = ipStr;
            //解析IP地址
            IpParser ipParser = new IpParser();
            try {
                //根据IP地址得出所在区域
                areaAddress = ipParser.parse(ipStr).split(" ")[0];
                localAddress = ipParser.parse(ipStr).split(" ")[1];
            } catch (Exception e) {
                e.printStackTrace();
            }

            //格式化请求接受时间
            DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
            try {
                Date date = df.parse(receiveTimeStr);
                //将时间转换为字符串
                receiveTime = Long.toString(date.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            //将url中的无效字符串丢弃
            urlStr = urlStr.substring(5);
            //重新拼装成url字符串
            url = hostNameStr + urlStr;

            //解析用户浏览器信息
            pattern = Pattern.compile(USER_AGENT_REGEX);
            matcher = pattern.matcher(userAgentStr);
            if (matcher.find()) {
//				for (int i = 0; i <= matcher.groupCount(); i++) {
//					System.out.println("group-" + i + " : " + matcher.group(i));
//				}
                //获取浏览器类型
                browserType = matcher.group(5);
                //获取操作系统类型
                operationSys = matcher.group(2).split(" ")[0];
            }

            //保存上一个页面url
            referUrl = referUrlStr;

            //HashMap保存cookie信息
            String[] strs = cookieStr.split(";");
            for (int i = 0; i < strs.length; i++) {
                String[] kv = strs[i].split("=");
                String keyStr = kv[0];
                String valStr = kv[1];
                cookies.put(keyStr, valStr);
            }
            //获取uuid信息
            uniqueId = cookies.get("uuid");
            //获取账号信息
            userId = cookies.get("userId");
            //如果没有获取成功，说明用户没有登录
            if (userId == null) {
                userId = "unlog_in";
            }
            //获取sessionTimes
            sessionTimes = cookies.get("st");
            //拼装成sessionId
            sessionId = uniqueId + "|" + sessionTimes;

        }
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getSessionTimes() {
        return sessionTimes;
    }

    public void setSessionTimes(String sessionTimes) {
        this.sessionTimes = sessionTimes;
    }

    public String getAreaAddress() {
        return areaAddress;
    }

    public void setAreaAddress(String areaAddress) {
        this.areaAddress = areaAddress;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public String getOperationSys() {
        return operationSys;
    }

    public void setOperationSys(String operationSys) {
        this.operationSys = operationSys;
    }

    public String getReferUrl() {
        return referUrl;
    }

    public void setReferUrl(String referUrl) {
        this.referUrl = referUrl;
    }

    public String getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(String receiveTime) {
        this.receiveTime = receiveTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getIndex() {
        return index;
    }

    /*
     * 测试
     */
    public static void main(String[] args) {
        //获取日志解析对象
        LogParser logParser = new LogParser();
        //解析测试用例日志
        logParser.parse(LogParser.LOG);

        String mapOutKey = logParser.getSessionId() + "&" + logParser.getReceiveTime();
        String mapOutValue = logParser.getIpAddress() + "\n" + logParser.getUniqueId() + "\n"
                + logParser.getUrl() + "\n" + logParser.getSessionId() + "\n"
                + logParser.getSessionTimes() + "\n" + logParser.getAreaAddress() + "\n"
                + logParser.getLocalAddress() + "\n" + logParser.getBrowserType() + "\n"
                + logParser.getOperationSys() + "\n" + logParser.getReferUrl() + "\n"
                + logParser.getReceiveTime() + "\n" + logParser.getUserId();
        System.out.println("mapOutKey: " + mapOutKey + "\nmapOutValue: " + mapOutValue);
    }
}
