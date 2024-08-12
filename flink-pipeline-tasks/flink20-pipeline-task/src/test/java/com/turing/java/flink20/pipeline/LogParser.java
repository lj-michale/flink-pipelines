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
 * Apache��������־Log����
 * https://blog.csdn.net/weixin_39469127/article/details/90419026
 * */
public class LogParser {

    //Apache��������־��Ϣ��������ʽ
    private static final String APACHE_LOG_REGEX =
            "^([0-9.]+)\\s([\\w.-]+)\\s([\\w.-]+)\\s\\[([^\\[\\]]+)\\]\\s\"((?:[^"
                    + "\"]|\\\")+)\"\\s(\\d{3})\\s(\\d+|-)\\s\"((?:[^\"]|\\\")+)\"\\s\"((?:"
                    + "[^\"]|\\\")+)\"\\s\"(.+)\"\\s(\\d+|-)\\s(\\d+|-)\\s(\\d+|-)\\s(.+)\\"
                    + "s(\\d+|-)$";

    //�û��������Ϣ��������ʽ
    private static final String USER_AGENT_REGEX = "^(.+)\\s\\((.+)\\)\\s(.+)\\s\\((.+)\\)\\s(.+)\\s(.+)$";

    //������־һ��
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

    //��Ҫ�������ֶ�
    private String ipAddress = null; //ip��ַ
    private String uniqueId = null; //uuid
    private String url = null; //���ʵ�url��ַ
    private String sessionId = null; //�Ựid
    private String sessionTimes = null; //��������Ӧʱ��
    private String areaAddress = null; //����
    private String localAddress = null; //����
    private String browserType = null; //���������
    private String operationSys = null; //����ϵͳ
    private String referUrl = null; //���ʵ���һ��ҳ��url
    private String receiveTime = null; //��������������ʱ��
    private String userId = null; //�û�id

    //����cookies��Ϣ��map
    private static Map<String, String> cookies = new HashMap<String, String>();

    //ʵ������LogParser����
    private static int count = 0;
    //LogParserΨһ��ʶ
    private int index = 0;
    private LogParser() {
        count++;
        index = count;
    }

    //��ʼ��Log������
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
     * ����һ��log��־
     *
     * @param log һ����־
     */
    public void parse(String log) {
        //��ʼ��Log������
        init();

        String ipStr = null;
        String receiveTimeStr = null;
        String urlStr = null;
        String referUrlStr = null;
        String userAgentStr = null;
        String cookieStr = null;
        String hostNameStr = null;

        //���������־
        Pattern pattern = Pattern.compile(APACHE_LOG_REGEX);
        Matcher matcher = pattern.matcher(LOG);
        if (matcher.find()) {
//			for (int i = 0; i <= matcher.groupCount(); i++) {
//				System.out.println("group-" + i + " : " + matcher.group(i));
//			}

            //����������ʽ����־�ļ��Ͽ�
            ipStr = matcher.group(1); //Զ���������ÿ�IP��ַ��
            receiveTimeStr = matcher.group(4); //��������������ʱ��
            urlStr = matcher.group(5); //����ĵ�һ�У�����ʽ�������URL����������Э�飩
            referUrlStr = matcher.group(8); //��һ�����ʵ�ҳ�棨�ÿ���Դ��
            userAgentStr = matcher.group(9); //�ÿ��������Ϣ
            cookieStr = matcher.group(10); //Cookie��Ϣ������uuid/userId/sessiontime��
            hostNameStr = matcher.group(14); //����������ַ

            //����IP��ַ
            ipAddress = ipStr;
            //����IP��ַ
            IpParser ipParser = new IpParser();
            try {
                //����IP��ַ�ó���������
                areaAddress = ipParser.parse(ipStr).split(" ")[0];
                localAddress = ipParser.parse(ipStr).split(" ")[1];
            } catch (Exception e) {
                e.printStackTrace();
            }

            //��ʽ���������ʱ��
            DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
            try {
                Date date = df.parse(receiveTimeStr);
                //��ʱ��ת��Ϊ�ַ���
                receiveTime = Long.toString(date.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            //��url�е���Ч�ַ�������
            urlStr = urlStr.substring(5);
            //����ƴװ��url�ַ���
            url = hostNameStr + urlStr;

            //�����û��������Ϣ
            pattern = Pattern.compile(USER_AGENT_REGEX);
            matcher = pattern.matcher(userAgentStr);
            if (matcher.find()) {
//				for (int i = 0; i <= matcher.groupCount(); i++) {
//					System.out.println("group-" + i + " : " + matcher.group(i));
//				}
                //��ȡ���������
                browserType = matcher.group(5);
                //��ȡ����ϵͳ����
                operationSys = matcher.group(2).split(" ")[0];
            }

            //������һ��ҳ��url
            referUrl = referUrlStr;

            //HashMap����cookie��Ϣ
            String[] strs = cookieStr.split(";");
            for (int i = 0; i < strs.length; i++) {
                String[] kv = strs[i].split("=");
                String keyStr = kv[0];
                String valStr = kv[1];
                cookies.put(keyStr, valStr);
            }
            //��ȡuuid��Ϣ
            uniqueId = cookies.get("uuid");
            //��ȡ�˺���Ϣ
            userId = cookies.get("userId");
            //���û�л�ȡ�ɹ���˵���û�û�е�¼
            if (userId == null) {
                userId = "unlog_in";
            }
            //��ȡsessionTimes
            sessionTimes = cookies.get("st");
            //ƴװ��sessionId
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
     * ����
     */
    public static void main(String[] args) {
        //��ȡ��־��������
        LogParser logParser = new LogParser();
        //��������������־
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
