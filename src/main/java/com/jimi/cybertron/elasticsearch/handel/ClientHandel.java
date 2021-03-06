package com.jimi.cybertron.elasticsearch.handel;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jimi.cybertron.elasticsearch.client.ClientUtil;
import com.jimi.cybertron.elasticsearch.thread.ThreadExecutorsAlarm;
import com.jimi.cybertron.elasticsearch.thread.ThreadExecutorsDevice;
import com.jimi.cybertron.elasticsearch.util.RandomDateUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * 使用java API操作elasticSearch
 *
 * @author chenx
 */
public class ClientHandel {

    private Logger logger = LoggerFactory.getLogger(ClientHandel.class);

    /**
     * 获取连接
     *
     * @throws Exception
     */
    public static TransportClient getConnection() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("cluster.name", "my-application");
        Settings.Builder settings = Settings.builder().put(map);
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.0.124"), Integer.parseInt("9300")));
        return client;
    }

    public void searchAndInsertDevice(Client client) throws Exception  {
        try {
            int count = 0;
            Connection con = null;
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            // mysql驱动
            con = (Connection) DriverManager.getConnection("jdbc:mysql://172.16.10.113:3306/test?useUnicode=true&characterEncoding=utf8",
                    "root", "123456");
            Statement ps = (Statement) con.createStatement();
            String sql = "select *  from user_relation";
            ResultSet rs = ps.executeQuery(sql);
            List<String> deviceList = new ArrayList<String>();
            while (rs.next()) {
                // 循环输出结果集
                JSONObject jsonObject =new JSONObject();
                jsonObject.put("user_parent_id",rs.getString("current_user_parent_id"));
                jsonObject.put("user_id",Integer.parseInt(rs.getString("user_id")));
                String platform_expiration_time=rs.getString("platform_expiration_time");
                jsonObject.put("platform_expiration_time",platform_expiration_time.substring(0,19));
                jsonObject.put("enable_flag",rs.getString("enable_flag"));
                String imei=rs.getString("imei")+4;
                jsonObject.put("imei",imei);
                logger.info("imei=========:"+imei);
                jsonObject.put("mc_type",rs.getString("mc_type"));
                jsonObject.put("app_id",rs.getString("app_id"));
                jsonObject.put("index_name","");
                jsonObject.put("repay_flag",rs.getString("repay_flag"));
                String result = JSON.toJSONString(jsonObject);
                deviceList.add(result);
                if (count++ > 10000) {
                    count = 0;
                    //启动线程池，批量提交
                    ThreadExecutorsDevice.exec(deviceList, client, "report_alarm_6", "device");
                    deviceList.clear();
                }
            }
        } catch (Exception e) {
            System.out.println("MYSQL error" + e.getMessage());
        }
    }

    /**
     * 查询全部内容，多线程插入
     **/
    public void searchAndInsertAlarm(Client client, String index, String table) throws InterruptedException {
        SearchResponse response = client.prepareSearch().setIndices(index).setTypes(table).get();
        int n = (int) response.getHits().totalHits();
        int size = 10000;
        int count = 0;
        List<String> retList = new ArrayList<String>();
        for(int total=4;total<5;total++) {
            for (int i = 0; i < n; ) {
                response = client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(1)).setFrom(i).setSize(size).get();
                i += size;
                SearchHits searchHits = response.getHits();
                for (SearchHit hit : searchHits) {
                    JSONObject jsonObject = JSONObject.parseObject(hit.getSourceAsString());
                    String imei=jsonObject.get("imei").toString()+total;
                    jsonObject.put("imei", imei);
                    jsonObject.put("create_time", RandomDateUtil.randomDate("2018-06-01", "2018-06-30"));
                    jsonObject.put("push_time", RandomDateUtil.randomDate("2018-06-01", "2018-06-30"));
                    String result = JSON.toJSONString(jsonObject);
                    logger.info("imei============:"+imei);
                    retList.add(result);
                    //logger.info("alarm_06_result");
                    if (count++ > 10000) {
                        count = 0;
                        //启动线程池，批量提交
                        ThreadExecutorsAlarm.exec(retList, client, "report_alarm_6", "alarm");
                        retList.clear();
                    }
                }
            }
        }
    }


    /**
     * 查询全部内容，多线程插入(从6库赋值device)
     **/
    public void searchAndInsertDevice(Client client, String index, String table) throws InterruptedException {
        SearchResponse response = client.prepareSearch().setIndices(index).setTypes(table).get();
        int n = (int) response.getHits().totalHits();
        int size = 10000;
        int count = 0;
        List<String> retList = new ArrayList<String>();
        for(int total=1;total<5;total++) {
            for (int i = 0; i < n; ) {
                response = client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(1)).setFrom(i).setSize(size).get();
                i += size;
                SearchHits searchHits = response.getHits();
                for (SearchHit hit : searchHits) {
                    JSONObject jsonObject = JSONObject.parseObject(hit.getSourceAsString());
                    String imeiExchange=jsonObject.get("imei").toString()+total;
                    jsonObject.put("imei", imeiExchange);
                    logger.info("imei========:"+imeiExchange);
                    String result = JSON.toJSONString(jsonObject);
                    retList.add(result);
                    if (count++ > 10000) {
                        count = 0;
                        //启动线程池，批量提交
                        ThreadExecutorsDevice.exec(retList, client, "report_alarm_6", "device");
                        retList.clear();
                    }
                }
            }
        }
    }


    /**
     * 从文件逐行读取写入es
     *
     public void writeEsFromFile(Client client) throws IOException {
     File article = new File("D:\\bulk.txt");
     FileReader fr=new FileReader(article);
     BufferedReader bfr=new BufferedReader(fr);
     String line=null;
     BulkRequestBuilder bulkRequest=client.prepareBulk();
     int count=0;
     while((line=bfr.readLine())!=null){
     JSONObject jsonObject=JSONObject.parseObject(line);
     bulkRequest.add(client.prepareIndex("tracker_test", "alarm", null)
     .setSource(JSON.toJSONString(jsonObject)));
     if (count++ >= 1) {
     count = 0;
     bulkRequest.execute(new ActionListener<BulkResponse>() {
    @Override public void onResponse(BulkResponse response) {
    // loger.info(response);
    }
    @Override public void onFailure(Throwable e) {
    e.printStackTrace();
    }
    });
     bulkRequest.get();
     bulkRequest = client.prepareBulk();
     }
     }
     bfr.close();
     fr.close();
     }*/

    /**
     * 查询结果直接写入
     **/
    private void searchAndWrite(Client client, String index, String table) throws InterruptedException {

        SearchResponse response = client.prepareSearch().setIndices(index).setTypes(table).get();
        int n = (int) response.getHits().totalHits();
        int size = 10000;
        List<String> alarmList = new ArrayList<String>();
        for (int i = 0; i < n; ) {
            response = client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(1)).setFrom(i).setSize(size).get();
            i += size;
            SearchHits searchHits = response.getHits();
            int count = 10000;
            long start = System.currentTimeMillis();
            for (SearchHit hit : searchHits) {

                JSONObject jsonObject = JSONObject.parseObject(hit.getSourceAsString());
                String imei=jsonObject.getString("imei");
                String user_id=jsonObject.getString("user_id");
                String id=jsonObject.getString("id");

                // 查询关键字
                QueryBuilder mpq1 = QueryBuilders
                        .matchPhraseQuery("id",id);
                QueryBuilder mpq2 = QueryBuilders
                        .matchPhraseQuery("imei",imei);
                QueryBuilder qb2 = QueryBuilders.boolQuery()
                        .must(mpq1)
//                .must(mpq3)
                        .must(mpq2);
                SearchResponse response2=client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(5)).setSize(size).setQuery(qb2).get();
                SearchHits searchHits2 = response2.getHits();
                    logger.info("imei:"+imei+"============id:"+id);
                    count--;
                    if(count==0){
                        break;
                    }
                }
                 logger.info("userTime:"+(System.currentTimeMillis()-start));
    }}


    /**
     * 使用过滤器查询，实现分页查询
     */
    public List<String> queryByFilter(Client client, String index, String table) {
        // 查询关键字
        QueryBuilder mpq1 = QueryBuilders
                .matchPhraseQuery("user_id","645945");
        QueryBuilder mpq2 = QueryBuilders
                .matchPhraseQuery("imei","868120185682520");
        QueryBuilder mpq3 = QueryBuilders
                .matchPhraseQuery("create_time","2018-06-22 13:02:06");
        QueryBuilder qb2 = QueryBuilders.boolQuery()
                .must(mpq1);
//                .must(mpq3)
//                .must(mpq2);
        SearchResponse response = client.prepareSearch().setIndices(index).setTypes(table).setQuery(qb2).get();
        int n = (int) response.getHits().totalHits();
        System.out.println(n);
        int size = 10000;
        List<String> retList = new ArrayList<String>();
        long start = System.currentTimeMillis();
        response = client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(5)).setSize(size).setQuery(qb2).get();
        SearchHits searchHits = response.getHits();
        for (SearchHit hit : searchHits) {
            logger.info("hit.getSourceAsString()====" + hit.getSourceAsString());
            retList.add(hit.getSourceAsString());
        }
        logger.info("useTime==："+(System.currentTimeMillis()-start));
        return retList;
    }


    public static void main(String[] args) throws Exception {


        ClientHandel javaESTest = new ClientHandel();;
        ClientUtil clientUtil = new ClientUtil();
        Client client = clientUtil.getClient();
//        //System.out.println("List========:"+javaESTest.searchAll(client,"report_alarm_7","alarm"));
//        //System.out.println("List========:"+javaESTest.searchAll(client,"tracker_201807","alarm"));
           //javaESTest.queryByFilter(client,"report_alarm_6","alarm");
        javaESTest.searchAndWrite(client,"report_alarm_6","alarm");
          //javaESTest.searchAndInsertDevice(client);
        //javaESTest.searchAndInsertDevice(client,"report_alarm_6","device");
          //javaESTest.searchAndInsertAlarm(client, "report_alarm_6", "alarm");
            //javaESTest.searchAndWrite(client, "report_alarm_6", "alarm");
       //javaESTest.queryByFilter(client,"tracker_201807", "alarm");
    }
}