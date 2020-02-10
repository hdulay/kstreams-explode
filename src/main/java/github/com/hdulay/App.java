package github.com.hdulay;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

class Data {
    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    String a;

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    String b;
}

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "explode-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "explode-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "localhost:8081");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());

        KStream<String, String> foo = builder.stream("foo");

        KStream<String, String> data = foo.flatMap((k, v) -> {
            JSONArray ja = new JSONArray(v);
            Stream<HashMap<String,Object>> list = ja.toList().stream().map(jo -> (HashMap) jo);

            List<KeyValue<String, String>> result = new ArrayList<>();

            list.forEach(map -> result.add(new KeyValue<String, String>(
                    map.get("a").toString(),
                    new JSONObject(map).toString(1)
            )));
            return result;
        });

        data.print(Printed.toSysOut());

        data.to("faltmapped");


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

//        streams.setUncaughtExceptionHandler((t, e) -> {
//            e.printStackTrace();
//        });

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
