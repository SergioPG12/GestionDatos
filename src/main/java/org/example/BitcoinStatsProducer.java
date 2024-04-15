package org.example;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

public class BitcoinStatsProducer {
    public static void main(String[] args) throws Exception {
        String topicName = "bitcoinStats";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            private final OkHttpClient client = new OkHttpClient();

            @Override
            public void run() {
                try {
                    String bitcoinPrice = fetchBitcoinPrice();
                    double bitcoinHashRate = fetchBitcoinHashRate();
                    String message = String.format("{\"time\":\"%d\", \"price\":%s, \"hashRate\":%.2f}",
                            System.currentTimeMillis(), bitcoinPrice, bitcoinHashRate);
                    producer.send(new ProducerRecord<>(topicName, "bitcoinKey", message));
                    System.out.println("Message sent successfully: " + message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private String fetchBitcoinPrice() throws Exception {
                Request request = new Request.Builder()
                        .url("https://api.coindesk.com/v1/bpi/currentprice.json")
                        .build();
                try (Response response = client.newCall(request).execute()) {
                    JSONObject jsonObject = new JSONObject(response.body().string());
                    return jsonObject.getJSONObject("bpi").getJSONObject("USD").getString("rate").replace(",", "");
                }
            }

            private double fetchBitcoinHashRate() throws Exception {
                Request request = new Request.Builder()
                        .url("https://api.blockchain.info/stats")
                        .build();
                try (Response response = client.newCall(request).execute()) {
                    JSONObject jsonObject = new JSONObject(response.body().string());
                    return jsonObject.getDouble("hash_rate");
                }
            }
        }, 0, 1000);
    }
}
