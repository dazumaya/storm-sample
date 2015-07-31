package com.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.guava.collect.Maps;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

import java.sql.Types;
import java.util.List;
import java.util.Map;

class SampleTopology {
    public static void main(String[] args) throws InterruptedException {
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        Map<String, Object> hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
        hikariConfigMap.put("dataSource.user", "root");
        //hikariConfigMap.put("dataSource.password","password");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        List<Column> columnSchema = Lists.newArrayList(
                new Column("line_id", Types.BIGINT),
                new Column("date", Types.VARCHAR),
                new Column("count", Types.BIGINT),
                new Column("count", Types.BIGINT));
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(columnSchema);

        JdbcState.Options options = new JdbcState.Options()
                .withConnectionPrvoider(connectionProvider)
                .withMapper(jdbcMapper)
                .withInsertQuery("insert into test.test (line_id, date, count) values (?, ?, ?) on duplicate key update count = count + ?;")
                .withQueryTimeoutSecs(30);
        JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);

        Fields fields = new Fields("line_id", "date");
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout1", spout)
                .each(spout.getOutputFields(), new Parse(), fields)
                .groupBy(fields)
                .aggregate(new Count(), new Fields("count"))
                .partitionPersist(jdbcStateFactory, new Fields("line_id", "date", "count"), new JdbcUpdater())
                .parallelismHint(6);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("reachCounter", conf, topology.build());
            Thread.sleep(100000);
            cluster.killTopology("reachCounter");
            cluster.shutdown();
        } else {
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology.build());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
