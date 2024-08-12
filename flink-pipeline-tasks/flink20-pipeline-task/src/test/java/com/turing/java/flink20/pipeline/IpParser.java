package com.turing.java.flink20.pipeline;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class IpParser {
    /*
     * IP��ַ������
     */
    //����IP���ݿ��ļ�
    private String DbPath = "./cz88/qqwry.dat";	//www.cz88.net

    private String Country, LocalStr;
    private long IPN;
    private int RecordCount, CountryFlag;
    private long RangE, RangB, OffSet, StartIP, EndIP, FirstStartIP, LastStartIP, EndIPOff;
    private RandomAccessFile fis;	//�ļ������д����byte[]��/д
    private byte[] buff;

    private long ByteArrayToLong(byte[] b) {
        long ret = 0;
        for (int i = 0; i < b.length; i++) {
            long t = 1L;
            for (int j = 0; j < i; j++) {
                t = t * 256L;
            }
            ret += ((b[i] < 0 ) ? 256 + b[i] : b[i]) * t;
        }
        return ret;
    }

    private long ipStrToLong(String ip) {
        String[] arr = ip.split("\\.");	//��������ʽ����"."�������������⺬�壬��ת��
        long ret = 0;
        for (int i = 0; i < arr.length; i++) {
            long l = 1;
            for (int j = 0; j < i; j++) {
                l *= 256L;
            }
            try {
                ret += Long.parseLong(arr[arr.length-i-1]) * l;	//�ӵ�λ���ұߣ���
            } catch(Exception e) {
                ret += 0;
            }
        }
        return ret;
    }

    public void seek(String ip) throws Exception {
        IPN = ipStrToLong(ip);
        fis = new RandomAccessFile(DbPath, "r");
        buff = new byte[4];
        fis.seek(0);	//seek() �����ļ�ָ��λ��
        fis.read(buff);	//��ȡbuff���ֽ�
        FirstStartIP = ByteArrayToLong(buff);
        fis.read(buff);
        LastStartIP = ByteArrayToLong(buff);
        RecordCount = (int)((LastStartIP - FirstStartIP) / 7);

        if (RecordCount <= 1) {
            LocalStr = Country = "δ֪";
            throw new Exception();
        }

        RangB = 0;
        RangE = RecordCount;
        long RecNo;

        do {
            RecNo = (RangB + RangE) / 2;
            getStartIP(RecNo);
            if (IPN == StartIP) {
                RangB = RecNo;
                break;
            }
            if (IPN > StartIP)
                RangB = RecNo;
            else
                RangE = RecNo;
        } while (RangB < RangE - 1);

        getStartIP(RangB);
        getEndIP();
        getCountry(IPN);

        fis.close();
    }

    private String getFlagStr(long OffSet) throws IOException {
        int flag = 0;
        do {
            fis.seek(OffSet);
            buff = new byte[1];
            fis.read(buff);
            flag = (buff[0] < 0) ? 256 + buff[0] : buff[0];
            if (flag == 1 || flag == 2) {
                buff = new byte[3];
                fis.read(buff);
                if (flag == 2) {
                    CountryFlag = 2;
                    EndIPOff = OffSet - 4;
                }
                OffSet = ByteArrayToLong(buff);
            } else {
                break;
            }
        } while (true);

        if (OffSet < 12) {
            return "";
        } else {
            fis.seek(OffSet);
            return getStr();
        }
    }

    private String getStr() throws IOException {
        long l = fis.length();
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        byte c = fis.readByte();
        do {
            byteout.write(c);
            c = fis.readByte();
        } while (c != 0 && fis.getFilePointer() < l);
        //qqwry.dat�ļ�Ĭ�ϱ���ΪGBK�������ļ���UTF-8��ʽ�ģ�������������룬��Ҫָ��ΪGBK
        return new String(byteout.toByteArray(),"GBK");
    }

    private void getCountry(long ip) throws IOException {
        if (CountryFlag == 1 || CountryFlag == 2) {
            Country = getFlagStr(EndIPOff + 4);
            if (CountryFlag == 1) {
                LocalStr = getFlagStr(fis.getFilePointer());
                if (IPN >= ipStrToLong("255.255.255.0") && IPN <= ipStrToLong("255.255.255.255")) {
                    LocalStr = getFlagStr(EndIPOff + 21);
                    Country = getFlagStr(EndIPOff + 12);
                }
            } else {
                LocalStr = getFlagStr(EndIPOff + 8);
            }
        } else {
            Country = getFlagStr(EndIPOff + 4);
            LocalStr = getFlagStr(fis.getFilePointer());
        }
    }

    private long getEndIP() throws IOException {
        fis.seek(EndIPOff);
        buff = new byte[4];
        fis.read(buff);
        EndIP = ByteArrayToLong(buff);
        buff = new byte[1];
        fis.read(buff);
        CountryFlag = (buff[0] < 0) ? 255 + buff[0] : buff[0];
        return EndIP;
    }

    private long getStartIP(long RecNo) throws IOException {
        OffSet = FirstStartIP + RecNo * 7;
        fis.seek(OffSet);
        buff = new byte[4];
        fis.read(buff);
        StartIP = ByteArrayToLong(buff);
        buff = new byte[3];
        fis.read(buff);
        EndIPOff = ByteArrayToLong(buff);
        return StartIP;
    }

    public String getLocal() { return this.LocalStr; }
    public String getCountry() { return this.Country; }
    public void setPath(String path) { this.DbPath = path; }

    //���øú������ɻ��IP��ַ���ڵ�ʵ������
    public String parse(String ipStr) throws Exception {
        this.seek(ipStr);
        return this.getCountry() + " " + this.getLocal();
    }


    //����
    public static void main(String[] args) {
        IpParser ipParser = new IpParser();
        try {
            //203.107.6.88
            String strs = ipParser.parse("120.196.145.58");
            System.out.println(strs.split(" ")[0] + " : " + strs.split(" ")[1]);
            //IP: 120.196.145.58 ���������㶫ʡ÷���� : �ƶ�
            //IP: 203.107.6.88 ���������㽭ʡ������ : ����ͰͰ�����NTP������
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
