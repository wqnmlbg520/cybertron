package com.jimi.cybertron.elasticsearch.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;


public class ClientUtil {
    private Client client;
    public ClientUtil() {
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化客户端
     * */
    private void init() throws Exception {
        byte[] bs = new byte[] { (byte) 172, (byte) 16, (byte)0, (byte)126 };
        Map<String, String> map = new HashMap<String, String>();
        map.put("cluster.name", "my-application");
        Settings.Builder settings = Settings.builder().put(map);
        this.client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(bs), 9300));
    }

    public Client getClient() {
        return client;
    }

}
