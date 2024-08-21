package com.tanla.frequency_capping;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Component
public class TestingClass implements CommandLineRunner {

    private static final String RECIPIENT_ID = "RECIPIENT_ID";

    private static final String SENDER_ID = "SENDER_ID";

    private static final String MINUTE_PRECISION = "_MINUTE_PRECISION_";
    private static final String SECOND_PRECISION = "_SECOND_PRECISION_";

    private static final String TIMESTAMP = "_TIMESTAMP";

    private static final String MARKETING = "_MARKETING";
    private static final String UTILITY = "_UTILITY";
    private static final String TRANSACTIONAL = "_TRANSACTIONAL";

    private static final String COMPRESSED = "_COMPRESSED";

    private static final String FIELD_1 = RECIPIENT_ID + UTILITY;
    private static final String FIELD_2 = RECIPIENT_ID + MARKETING;
    private static final String FIELD_3 = RECIPIENT_ID + TRANSACTIONAL;

    @Autowired
    private JedisPool jedisPool;

    @Override
    public void run(String... args) throws Exception {

        // System.out.println("Size of the localDate : " +
        // ClassLayout.parseClass(LocalDate.class).toPrintable());
        // System.out.println("Size of the TimeStamp : " +
        // ClassLayout.parseClass(Timestamp.class).toPrintable());
        // System.out.println("Size of the TimeStamp : " + LocalDateTime.now());
        // System.out.println("Size of the localDateTime : " +
        // ClassLayout.parseClass(LocalDateTime.class).toPrintable());
        // System.out.println("Size of the System.currentMillis() : " + Long.SIZE);

        int nofBytesInTimeStamp = 4;

        long time = System.currentTimeMillis();
        long timeSeconds = time / 1000;
        long timeMinutes = time / (1000 * 60);

        println("Time stamp in seconds " + Long.toString(timeSeconds));
        println("Time stamp in minutes " + Long.toString(timeMinutes));

        byte[] byteArrayForSeconds = longToByte(timeSeconds);
        byte[] byteArrayForSecondsCompressed = Arrays.copyOfRange(byteArrayForSeconds, 8 - nofBytesInTimeStamp, 8);
        byte[] byteArrayForMinutes = longToByte(timeMinutes);
        byte[] byteArrayForMinutesCompressed = Arrays.copyOfRange(byteArrayForMinutes, 8 - nofBytesInTimeStamp, 8);

        println("The array of bytes of seconds : ");
        printByteArrayToConsole(byteArrayForSecondsCompressed);

        println("The array of bytes of minutes : ");
        printByteArrayToConsole(byteArrayForMinutesCompressed);

        long x = bytesToLong(byteArrayForSecondsCompressed, nofBytesInTimeStamp);
        println("\nThe long value back from timeSeconds : " + x);
        long a = bytesToLong(byteArrayForMinutesCompressed, nofBytesInTimeStamp);
        println("\nThe long value back from timeMinutes : " + a);

        createJedisResourcesSecondPrecisionCompressed(byteArrayForSecondsCompressed, 20);
        createJedisResourcesMinutePrecisionCompressed(byteArrayForMinutesCompressed, 20);
        createJedisResourcesSecondPrecisionCompressed(byteArrayForSecondsCompressed, 100);
        createJedisResourcesMinutePrecisionCompressed(byteArrayForMinutesCompressed, 100);
        createJedisResourcesUncompressed(20, timeSeconds, SECOND_PRECISION);
        createJedisResourcesUncompressed(20, timeMinutes, MINUTE_PRECISION);
        createJedisResourcesUncompressed(100, timeSeconds, SECOND_PRECISION);
        createJedisResourcesUncompressed(100, timeMinutes, MINUTE_PRECISION);
    }

    private void createJedisResourcesUncompressed(Integer noOfTimeStamps, Long timestamp, String precision)
            throws Exception {

        println("In createJedisResourcesUncompressed");
        try (Jedis jedis = this.jedisPool.getResource()) {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < noOfTimeStamps / 3; i++) {
                String temp = timestamp.toString() + "/";
                builder.append(temp);
            }

            String key = SENDER_ID + precision + noOfTimeStamps.toString() + TIMESTAMP;

            jedis.hset(key.getBytes(), FIELD_1.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_2.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_3.getBytes(), convertStringBuilderToString(builder).getBytes());

        }
    }

    private void createJedisResourcesSecondPrecisionCompressed(byte[] byteArrayForSeconds, Integer noOfTimeStamps)
            throws Exception {

        println("In createJedisResourcesSecondPrecisionCompressed");
        try (Jedis jedis = this.jedisPool.getResource()) {
            StringBuilder builder = new StringBuilder("");

            for (int i = 0; i < noOfTimeStamps / 3; i++) {
                for (byte b : byteArrayForSeconds) {
                    builder.append(b);
                }
                // builder.append("/");
            }

            String key = SENDER_ID + SECOND_PRECISION + noOfTimeStamps.toString() + TIMESTAMP + COMPRESSED;

            jedis.hset(key.getBytes(), FIELD_1.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_2.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_3.getBytes(), convertStringBuilderToString(builder).getBytes());
        }
    }

    private void createJedisResourcesMinutePrecisionCompressed(byte[] byteArrayForMinutes, Integer noOfTimeStamps)
            throws Exception {

        println("In createJedisResourcesMinutePrecisionCompressed");
        try (Jedis jedis = this.jedisPool.getResource()) {
            StringBuilder builder = new StringBuilder("");

            for (int i = 0; i < noOfTimeStamps / 3; i++) {
                for (byte b : byteArrayForMinutes) {
                    builder.append(b);
                }
                // builder.append("/");
            }

            String key = SENDER_ID + MINUTE_PRECISION + noOfTimeStamps.toString() + TIMESTAMP + COMPRESSED;

            jedis.hset(key.getBytes(), FIELD_1.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_2.getBytes(), convertStringBuilderToString(builder).getBytes());
            jedis.hset(key.getBytes(), FIELD_3.getBytes(), convertStringBuilderToString(builder).getBytes());
        }
    }

    public String convertStringBuilderToString(StringBuilder stringBuilder) {
        return stringBuilder.toString();
    }

    public void println(String message) {
        System.out.println(message);
    }

    public void printByteArrayToConsole(byte[] bytes) {
        for (byte b : bytes) {
            System.out.print(b + "/");
        }
        System.out.println();
    }

    public byte[] longToByte(Long c) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(c);
        return buffer.array();
    }

    public long bytesToLong(byte[] bytes, int nofBytesInTimeStamp) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.position(Long.BYTES - nofBytesInTimeStamp);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

    public long bytesToLongMinutes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.position(2);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }
}
