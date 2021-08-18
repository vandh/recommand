package com.demo.task;

import com.demo.map.UserHistoryMapFunction;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class UserHistoryTask {
    //当用户点击商品浏览的链接时，会触发/log向kafka的主题con发送消息，此时则会消费此条消息
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("history");
        //从kafka读取数据
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        //该用户下的产品浏览数加1，该产品下的用户浏览数加1
        dataStream.map(new UserHistoryMapFunction());

        env.execute("User Product History");
    }
}
