package com.jimi.cybertron.elasticsearch.thread;

import com.alibaba.fastjson.JSONObject;
import com.jimi.cybertron.elasticsearch.handel.ClientHandel;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ThreadExecutorsAlarm {


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
        BulkRequestBuilder bulkRequest=client.prepareBulk();
            try {
                for (int i = 0; i < list.size(); i++) {
                //这里还要说一下，，由于在实质项目中，当处理的数据存在等待超时和出错会使线程一直处于等待状态
                //这里只是处理简单的，
                    //logger.info("list[i]========"+list.get(i));
//                    bulkRequest.add(client.prepareIndex(index, table, "null")
//                            .setSource(list.get(i)));
                    JSONObject jsonObject= JSONObject.parseObject(list.get(i));
                    bulkRequest.add(client.prepareIndex("report_alarm_6", "alarm", null).setTTL(TimeValue.timeValueHours(999999999))
                            .setParent(jsonObject.getString("imei")).setSource(list.get(i)).request());

                        bulkRequest.execute(new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                 //loger.info(response);
                            }
                            @Override
                            public void onFailure(Throwable e) {
                                e.printStackTrace();
                            }
                        });
                        bulkRequest.get();
                        bulkRequest = client.prepareBulk();
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
