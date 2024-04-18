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
    // Define el número máximo de observaciones que la serie de tiempo puede tener.
    private static final int MAX_OBSERVATIONS = 10000;

    private TimeSeries priceSeries = new TimeSeries("Precio de Bitcoin");
    private TimeSeries hashRateSeries = new TimeSeries("Tasa de Hash de Bitcoin");
    private JFreeChart chart;

    public BitcoinStatsConsumer() {
        super("Visor de Estadísticas de Bitcoin");
        setIcon();
        initChart();
        pack();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);
        consumeKafka();
    }

    private void setIcon() {
        // Configuración del icono de la aplicación usando una imagen desde internet.
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
        // Configuración inicial del gráfico que incluye la serie de precios y la tasa de hash.
        TimeSeriesCollection priceDataset = new TimeSeriesCollection(priceSeries);
        TimeSeriesCollection hashRateDataset = new TimeSeriesCollection(hashRateSeries);

        chart = ChartFactory.createTimeSeriesChart(
                "Evolución del Precio vs Tasa de Hash de Bitcoin",
                "Tiempo",
                "Precio de Bitcoin (USD)",
                priceDataset,
                true,
                true,
                false
        );

        XYPlot plot = chart.getXYPlot();

        // Configuración del eje secundario para la tasa de hash.
        NumberAxis hashRateAxis = new NumberAxis("Tasa de Hash");
        plot.setRangeAxis(1, hashRateAxis);
        plot.setDataset(1, hashRateDataset);
        plot.mapDatasetToRangeAxis(1, 1);

        // Configuración de colores diferenciados para cada serie de datos.
        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer(true, false);
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer(true, false);
        renderer1.setSeriesPaint(0, Color.RED);
        renderer2.setSeriesPaint(0, Color.BLUE);

        plot.setRenderer(0, renderer1);
        plot.setRenderer(1, renderer2);

        // Añadir el panel del gráfico al JFrame.
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 400));
        setContentPane(chartPanel);

        // Establece la cantidad máxima de observaciones en la serie de tiempo.
        priceSeries.setMaximumItemCount(MAX_OBSERVATIONS);
        hashRateSeries.setMaximumItemCount(MAX_OBSERVATIONS);
    }

    private void consumeKafka() {
        // Configuración del consumidor de Kafka.
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
        // Procesamiento y actualización del gráfico con los nuevos datos recibidos.
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
        // Corrección de formato numérico para adaptar la salida JSON a las necesidades del parser.
        return jsonText.replaceAll("(\\d+),(\\d+)", "$1.$2");
    }

    public static void main(String[] args) {
        new BitcoinStatsConsumer();
    }
}
