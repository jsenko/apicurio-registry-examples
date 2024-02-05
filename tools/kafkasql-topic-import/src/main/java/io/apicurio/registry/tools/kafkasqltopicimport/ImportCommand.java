/*
 * Copyright 2023 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.apicurio.registry.tools.kafkasqltopicimport;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import picocli.CommandLine.Command;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static picocli.CommandLine.Option;

/**
 * @author Jakub Senko <em>m@jsenko.net</em>
 */
@Command(name = "import", version = "0.1", mixinStandardHelpOptions = true)
public class ImportCommand implements Runnable {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Option(names = {"-b", "--bootstrap-sever"}, description = "Kafka bootstrap server URL.",
            required = true, defaultValue = "localhost:9092")
    private String kafkaBootstrapServer;

    @Option(names = {"-f", "--file"}, description = "Path to a kafkasql-journal topic dump file. " +
            "Messages must use a JSON envelope and have base64-encoded keys and values.", required = true)
    private String dumpFilePath;

    @Option(names = {"-d", "--debug"}, description = "Print debug log messages.", defaultValue = "false")
    private boolean debug;

    public void run() {

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

//        if (debug) {
//            System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
//        } else {
//            System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "WARN");
//        }

        // Read data
        //Dump dump = null;
        List<Envelope> dump = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dumpFilePath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                var dumpLine = mapper.readValue(line, Envelope.class);
                dump.add(dumpLine);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

/*
        try (var in = new FileInputStream(dumpFilePath)) {
            //var data = Files.readString(Path.of(dumpFilePath));

            //dump = mapper.readValue(in, Dump.class);
            dump = mapper.readValue(in, new TypeReference<List<Envelope>>() {
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
*/

        try (Producer<byte[], byte[]> producer = createKafkaProducer()) {

            for (Envelope envelope : dump) {


                if (envelope.getHeaders() == null) {
                    envelope.setHeaders(List.of());
                }


                var key = envelope.getKey().getBytes();
                byte[] value = null;
                if (envelope.getPayload() != null) {
                    value = fixCorruptedData(envelope.getPayload().getBytes());
                }

                // Read headers
                if (envelope.getHeaders().size() % 2 != 0) {
                    throw new RuntimeException("Invalid length of the headers field " + envelope.getHeaders().size() + " at " + envelope.getOffset());
                }
                List<Header> headers = new ArrayList<>();
                for (int i = 0; i < envelope.getHeaders().size(); i += 2) {
                    headers.add(new RecordHeader(envelope.getHeaders().get(i), envelope.getHeaders().get(i + 1).getBytes()));
                }

                var record = new ProducerRecord<>(
                        "kafkasql-journal", //envelope.getTopic(),
                        envelope.getPartition(),
                        envelope.getTs(),
                        key,
                        value,
                        headers
                );
                producer.send(record);

            }

            producer.flush();
            System.err.println("Data imported successfully.");

        } catch (Exception ex) {
            System.err.println("Data import failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }


    private Producer<byte[], byte[]> createKafkaProducer() {

        Properties props = new Properties();

        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "Producer-kafkasql-journal");
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        //props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }


    private byte[] fixCorruptedData(byte[] data) {

        var ouput = ByteBuffer.allocate(data.length);
/*
        // ===================== Write content for debugging
        var out = "[";
        var first = true;
        for (byte b : data) {
            int bb = b >= 0 ? b : 256 + b;
            if (!first) {
                out = out + ", ";
            }
            first = false;
            out = out + (bb == 0 ? " " : "") + (bb < 100 ? " " : "") + bb;
        }
        out = out + "]";
        System.err.println("content data: " + out);
        // =====================
*/

        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byte type = byteBuffer.get(); // the first byte is the message type ordinal, skip that
        ouput.put(type);

        if (type != 2) {
            // not content
            return data;
        }

        byte actionOrdinal = byteBuffer.get();
        ouput.put(actionOrdinal);

        // Canonical hash (length of string + string bytes)
        String canonicalHash = null;
        int hashLen = byteBuffer.getInt();
        ouput.putInt(hashLen);
        if (hashLen > 0) {
            // hash should be 64 (@) characters
            if(hashLen != 64) {
                throw new RuntimeException("canonical hash length is " + hashLen);
            }
            byte[] bytes = new byte[hashLen];
            byteBuffer.get(bytes);
            ouput.put(bytes);
            canonicalHash = new String(bytes, StandardCharsets.UTF_8);
        }

        // TODO: There should be no references in this version, making things easier

        // Skip the next four bytes
        byteBuffer.position(byteBuffer.position() + 4);

        // TODO: Some content starts with 0xBFBD7B, which is two junk (?) bytes and then "{"
        // TODO: Remove them?


        var content = new byte[byteBuffer.remaining()];
        byteBuffer.get(content);
        ouput.putInt(content.length);
        ouput.put(content);
/*
        // =====================
        // We need to recalculate corrupted content sizes.
        // We take advantage of the content size being written in four bytes, but very
        // rarely being that large.
        var buff = byteBuffer.duplicate();
        var bbytes = new byte[buff.remaining()];
        buff.get(bbytes);
        // look for 2 null bytes, from reverse
        var reference_count_i = -1;
        var content_count_i = -1;
        for (int i = bbytes.length - 2; i >= 0; i--) {
            if (bbytes[i] == 0 && bbytes[i + 1] == 0) {
                if (i == 0 || bbytes[i - 1] != 0) {
                    // Found 2 zero bytes preceding a non-zero (or no) byte, it is either reference count, content count, or bogus
                    if (reference_count_i == -1) {
                        reference_count_i = i;
                    } else if (content_count_i == -1) {
                        content_count_i = i;
                    } else {
                        throw new RuntimeException("bogus");
                    }
                }
            }
        }
        // Mark the count as content if there are no references
        if (reference_count_i != -1 && content_count_i == -1) {
            content_count_i = reference_count_i;
            reference_count_i = -1;
        }
        // Now fix the byte values
        // Fix the content length
        if (content_count_i != -1) {
            if (reference_count_i == -1) {
                var size = bbytes.length - content_count_i - 4;
                byteBuffer.putInt(byteBuffer.position() + content_count_i, size);
            } else {
                var size = reference_count_i - content_count_i - 4;
                byteBuffer.putInt(byteBuffer.position() + content_count_i, size);
            }
        }
        // Fix the references length
        if (reference_count_i != -1) {
            var size = bbytes.length - reference_count_i - 4;
            byteBuffer.putInt(byteBuffer.position() + reference_count_i, size);
        }
        // =====================


        // Content (length of content + content bytes)
        String content = null;
        int numContentBytes = byteBuffer.getInt();


//        if ("67c1f422fa17fe80374615eac0f9d6aeac99b7f4f26d89cf2c0dfc2ee9ebc998".equals(canonicalHash)) {
//            // Fix the two weird bytes
//            // [191, 189]
//            // 0xBFBD
//            byteBuffer.get();
//            numContentBytes--;
//            byteBuffer.get();
//            numContentBytes--;
//        }
//
//
//        if ("9ed43cb1f094c032a802e51e5e0da44a857f6b78d22336e16f15d4ae53a711e8".equals(canonicalHash)) {
//            // Fix the two weird bytes
//            // [191, 189]
//            // 0xBFBD
//            byteBuffer.get();
//            numContentBytes--;
//            byteBuffer.get();
//            numContentBytes--;
//        }

        ouput.putInt(numContentBytes);
        if (numContentBytes > 0) {
            byte[] contentBytes = new byte[numContentBytes];
            byteBuffer.get(contentBytes);
            ouput.put(contentBytes);
            content = new String(contentBytes);
        }

        String serializedReferences = null;
        //When deserializing from other storage versions, the references byte count might not be there, so we first check if there are anything remaining in the buffer
        if (byteBuffer.hasRemaining()) {
            // References (length of references + references bytes)
            int referencesLen = byteBuffer.getInt();
            ouput.putInt(referencesLen);
            if (referencesLen > 0) {
                // TODO This should not happen in this version
                throw new RuntimeException("references should not be there");
                //byte[] bytes = new byte[referencesLen];
                //byteBuffer.get(bytes);
                //ouput.put(bytes);
                //serializedReferences = new String(bytes, StandardCharsets.UTF_8);
            }
        }
*/
        if (byteBuffer.hasRemaining()) {
            throw new RuntimeException("has remaining");
        }

        ouput.flip();
        var res = new byte[ouput.remaining()];
        ouput.get(res);
        return res;
    }
}
