package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StoreQueryParameters;

import java.util.Properties;

public class App {
    private static final String TOPIC_NAME = "user-catalog";
    private static final String STORE_NAME = "user-bootstrap-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-replica-group-" + System.currentTimeMillis()); // Уникальный
                                                                                                            // ID для
                                                                                                            // каждого
                                                                                                            // инстанса
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Важно: начинаем чтение с самого начала топика, чтобы собрать все 300к записей
        props.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Создаем полную локальную реплику через GlobalKTable
        GlobalKTable<String, String> userTable = builder.globalTable(
                TOPIC_NAME,
                Materialized.as(STORE_NAME));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Логгер состояния
        streams.setStateListener((newState, oldState) -> {
            System.out.println("Статус стрима изменился: " + oldState + " -> " + newState);
        });

        System.out.println("🚀 Запуск репликации каталога пользователей...");
        streams.start();

        // 2. Фоновый поток для мониторинга наполнения реплики
        new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(5000);

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        // Используем StoreQueryParameters для типизированного доступа
                        ReadOnlyKeyValueStore<String, String> store = streams.store(
                                StoreQueryParameters.fromNameAndType(
                                        STORE_NAME,
                                        QueryableStoreTypes.keyValueStore()));

                        long count = store.approximateNumEntries();
                        System.out.println("📊 Локальная реплика содержит ~" + count + " пользователей");

                        String testUser = store.get("user_100");
                        if (testUser != null) {
                            System.out.println("✅ Пример данных (ID 100): " + testUser);
                        }
                    }

                }
            } catch (Exception e) {
                System.err.println("Ошибка при чтении из State Store: " + e.getMessage());
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
