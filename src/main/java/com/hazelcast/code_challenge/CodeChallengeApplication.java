package com.hazelcast.code_challenge;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@SpringBootApplication
public class CodeChallengeApplication {

    public static final String INSTANCE_NAME = "unicorn";
    public static final String MAP_NAME = "node-map";

    public static final String CLIENT_BIND_ADDRESS = "127.0.0.1:8090";
    public static final String CLUSTER_BIND_INTERFACE = "192.168.1.*";

    public static final String MULTICAST_GROUP = "224.2.1.5";
    public static final int MULTICAST_PORT = 9091;
    public static final int MULTICAST_TIME_TO_LIVE = 32;

    public static final int TIME_TO_NAP = 15;

    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        SpringApplication.run(CodeChallengeApplication.class, args);

        // attempt to join an existing cluster
        final var client = joinCluster();

        if (client != null) {

            System.out.println("Joined cluster. Map size: " + client.getMap(MAP_NAME).size());
            Thread.sleep(5000);
            client.shutdown();
        } else {
            // otherwise create a cluster
            var hazelcastInstance = createCluster();

            IMap<UUID, String> nodeMap = hazelcastInstance.getMap(MAP_NAME);

            if (nodeMap.size() == 0) {
                System.out.println("We are started!");
            }
            nodeMap.put(UUID.randomUUID(), generateNodeIdentifier());

            System.out.println("Taking a nap for " + TIME_TO_NAP + " minutes: " + generateNodeIdentifier());
            Thread.sleep(TIME_TO_NAP * 60 * 1000);
        }
        Hazelcast.shutdownAll();
    }

    private static HazelcastInstance createCluster() {
        final var config = buildConfig();

        final MulticastConfig mcConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        mcConfig.setMulticastGroup(MULTICAST_GROUP);
        mcConfig.setMulticastPort(MULTICAST_PORT);
        mcConfig.setMulticastTimeToLive(MULTICAST_TIME_TO_LIVE);

        return Hazelcast.getOrCreateHazelcastInstance(config);
    }

    private static String generateNodeIdentifier() throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        return (new StringBuilder(ip.getHostName())
                .append("-")
                .append(ip)
                .append("-")
                .append(System.currentTimeMillis())
        )
                .toString();
    }

    private static HazelcastInstance joinCluster() {
        HazelcastInstance client = null;
        try {
            client = HazelcastClient.newHazelcastClient(buildClientConfig());
        } catch (IllegalStateException e) {
            System.err.println("Failed to start the client as there is no cluster to join " + e.getMessage());
        }
        return client;
    }

    private static ClientConfig buildClientConfig() {
        var clientConfig = new ClientConfig();
        clientConfig.setInstanceName(INSTANCE_NAME);
        clientConfig.getNetworkConfig().addAddress(CLIENT_BIND_ADDRESS);
        clientConfig.setProperty("hazelcast.socket.client.bind.any", "true");

        return clientConfig;
    }

    private static Config buildConfig() {
        var config = new Config();
        config.setProperty("hazelcast.socket.server.bind.any", "false");
        config.setInstanceName(INSTANCE_NAME);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config.getNetworkConfig().getInterfaces().setEnabled(true);
        config.getNetworkConfig().getInterfaces().addInterface(CLUSTER_BIND_INTERFACE);
        return config;
    }
}
