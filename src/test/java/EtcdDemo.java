import com.milvus.common.EtcdClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 测试服务
 * 节点向etcd中注册，
 * 节点在etcd中的查询
 */
public class EtcdDemo {
    private static final Logger logger = LoggerFactory.getLogger(EtcdClient.class);

    public static void main(String[] args) throws Exception {
        EtcdClient etcd = new EtcdClient("http://localhost:2379");

        // 异步注册服务
        CompletableFuture.allOf(
                etcd.registerServiceAsync("query-node", "node1:7000", 30),
                etcd.registerServiceAsync("query-node", "node2:7001", 30)
        ).thenCompose(v -> {
            // 服务发现
            etcd.discoverServiceWithRetry("query-node",5,300)
                    .thenAccept(services->{
                        if (services.isEmpty()){
                            logger.warn("No query nodes found after retries");
                        }else {
                            logger.info("Query-Nodes:" + services);
                        }
                    })
                    .exceptionally(ex->{
                        logger.error("Failed to discover query nodes:{}",ex.getMessage());
                        return null;
                    });

            // 配置操作
            return etcd.put("config/max-connections", "1000")
                    .thenCompose(__ -> etcd.getAll("config/"))
                    .thenAccept(configs -> {
                        logger.info("All configs:" + configs);
                        etcd.close();
                    });
        }).exceptionally(ex -> {
            System.err.println("Operation failed: " + ex.getMessage());
            etcd.close();
            return null;
        }).get(20, TimeUnit.SECONDS);
    }
}
