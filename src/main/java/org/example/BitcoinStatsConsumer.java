package org.example;

import org.apache.kafka.clients.consumer.*;
import org.jfree.chart.*;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.*;
import javax.swing.*;
import java.awt.*;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import org.json.JSONObject;
import org.json.JSONException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class BitcoinStatsConsumer extends JFrame {
    private TimeSeries priceSeries = new TimeSeries("Bitcoin Price");
    private TimeSeries hashRateSeries = new TimeSeries("Bitcoin Hash Rate");
    private JFreeChart chart;

    public BitcoinStatsConsumer() {
        super("Bitcoin Stats Viewer");
        setIcon();
        initChart();
        pack();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);
        consumeKafka();
    }
    private void setIcon() {
        // Establecer ícono de la ventana desde una URL
        try {
            URL iconUrl = new URL("https://cryptologos.cc/logos/bitcoin-btc-logo.png");
            BufferedImage iconImage = ImageIO.read(iconUrl);
            setIconImage(iconImage);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error al cargar la imagen del ícono desde la web.");
        }
    }

    private void initChart() {
        TimeSeriesCollection priceDataset = new TimeSeriesCollection(priceSeries);
        TimeSeriesCollection hashRateDataset = new TimeSeriesCollection(hashRateSeries);

        chart = ChartFactory.createTimeSeriesChart(
                "Bitcoin Price vs Hash Rate Evolution",
                "Time",
                "Bitcoin Price (USD)",
                priceDataset,
                true,
                true,
                false
        );

        XYPlot plot = chart.getXYPlot();

        // Set the hash rate axis on the right
        NumberAxis hashRateAxis = new NumberAxis("Hash Rate");
        plot.setRangeAxis(1, hashRateAxis);
        plot.setDataset(1, hashRateDataset);
        plot.mapDatasetToRangeAxis(1, 1);

        // Set different colors for the two series
        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer(true, false);
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer(true, false);
        renderer1.setSeriesPaint(0, Color.RED);
        renderer2.setSeriesPaint(0, Color.BLUE);

        plot.setRenderer(0, renderer1);
        plot.setRenderer(1, renderer2);

        // Add the chart panel to the window
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 400));
        setContentPane(chartPanel);

    }

    private void consumeKafka() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bitcoinStatsGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("bitcoinStats"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record);
                }
            }
        }
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            String correctedJson = correctNumberFormat(record.value());
            JSONObject json = new JSONObject(correctedJson);
            long time = json.getLong("time");
            double price = json.getDouble("price");
            double hashRate = json.getDouble("hashRate");

            priceSeries.addOrUpdate(new Second(new Date(time)), price);
            hashRateSeries.addOrUpdate(new Second(new Date(time)), hashRate);
        } catch (JSONException e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String correctNumberFormat(String jsonText) {
        // Esta expresión regular busca números que contienen comas que podrían ser decimales
        return jsonText.replaceAll("(\\d+),(\\d+)", "$1.$2");
    }


    public static void main(String[] args) {
        new BitcoinStatsConsumer();

    }
}