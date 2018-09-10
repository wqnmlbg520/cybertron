package com.jimi.cybertron.elasticsearch.thread;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ThreadExecutors_test {


    //测试
    public static void main(String[] args) {
//        List<String> list = new ArrayList<String>();
//        //数据越大线程越多
//        for (int i = 0; i < 3000000; i++) {
//            list.add("hello" + i);
//        }
//        try {
//            exec(list);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public static void exec(List<String> list,Client client,String index,String table) throws InterruptedException {
        int count = 300;//一个线程处理300条数据
        int listSize = list.size();//数据集合大小
        int runSize = (listSize / count) + 1; //开启的线程数
        List<String> newlist = null;//存放每个线程的执行数据
        ExecutorService executor = Executors.newFixedThreadPool(runSize);//创建一个线程池，数量和开启线程的数量一样
        //创建两个个计数器
        CountDownLatch begin = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(runSize);
        //循环创建线程
        for (int i = 0; i < runSize; i++) {
            //计算每个线程执行的数据
            if ((i + 1) == runSize) {
                int startIndex = (i * count);
                int endIndex = list.size();
                newlist = list.subList(startIndex, endIndex);
            } else {
                int startIndex = (i * count);
                int endIndex = (i + 1) * count;
                newlist = list.subList(startIndex, endIndex);
            }
            //线程类
            MyThread mythead = new MyThread(newlist, begin, end,client,index,table);
            //这里执行线程的方式是调用线程池里的executor.execute(mythead)方法。
            executor.execute(mythead);
        }

        begin.countDown();
        end.await();

        //执行完关闭线程池
        executor.shutdown();
    }

    //线程类就不另外写了，写在同一个文件里咯，有需要可以分开写
    private static class MyThread implements Runnable {
        private List<String> list;
        private CountDownLatch begin;
        private CountDownLatch end;
        private Client client;
        private String index;
        private String table;

        //logback
        private Logger logger = LoggerFactory.getLogger(MyThread.class);


        //创建个构造函数初始化 list,和其他用到的参数
        public MyThread(List<String> list, CountDownLatch begin, CountDownLatch end,Client client,String index,String table) {
            this.list = list;
            this.begin = begin;
            this.end = end;
            this.client = client;
            this.index = index;
            this.table = table;
        }



        @Override
        public void run() {
            try {
                for (int i = 0; i < list.size(); i++) {
                    JSONObject jsonObject= JSONObject.parseObject(list.get(i));
                    //logger.info("user_id:"+jsonObject.getString("user_id")+"============imei:"+jsonObject.getString("imei")+"ThreadNo:"+Thread.currentThread().getName());

                    // 查询关键字
                    QueryBuilder mpq1 = QueryBuilders
                            .matchPhraseQuery("user_id", jsonObject.getString("user_id"));
                    QueryBuilder mpq2 = QueryBuilders
                            .matchPhraseQuery("imei", jsonObject.getString("imei"));
                    QueryBuilder mpq3 = QueryBuilders
                            .matchPhraseQuery("create_time", "2018-06-22 13:02:06");
                    QueryBuilder qb2 = QueryBuilders.boolQuery()
                            .must(mpq1)
//                .must(mpq3)
                .must(mpq2);
                    SearchResponse response = client.prepareSearch().setIndices(index).setTypes(table).setQuery(qb2).get();
                    int n = (int) response.getHits().totalHits();
                    System.out.println(n);
                    int size = 10000;
                     response = client.prepareSearch().setIndices(index).setTypes(table).setScroll(TimeValue.timeValueMinutes(5)).setSize(n).setQuery(qb2).get();
                    SearchHits searchHits = response.getHits();
//                    for (SearchHit hit : searchHits) {
//                        logger.info("hit.getSourceAsString()====" + hit.getSourceAsString());
//                    }

                }
                //执行完让线程直接进入等待
                begin.await();
            } catch (InterruptedException e) {
                logger.error("thread_report_alarm_6====InterruptedException:"+e.toString());
            } finally {
            //这里要主要了，当一个线程执行完 了计数要减一不要这个线程会被一直挂起,end.countDown()，这个方法就是直接把计数器减一的
                end.countDown();
            }
        }


    }


}
