package lu.pistache.avro.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lu.pistache.avro.AvroObjectCreationException;
import lu.pistache.avro.AvroTools;
import lu.pistache.avro.ParseXmlException;
import lu.pistache.avro.Tools;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Use this class to try to write something to Kafka
 */
public class KafkaWriteTest {


    public static void main(String[] args) throws IOException, AvroObjectCreationException, ParseXmlException {
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prodProps.put("schema.registry.url", "http://localhost:8081");

        String avroSchemaString = Tools.readFile("src/test/resources/lu/pistache/avro/kafka/User.avsc");
        String inputXml = Tools.readFile("src/test/resources/lu/pistache/avro/kafka/input.xml");

        Schema schema = AvroTools.createSchemaFromString(avroSchemaString);

        inputXml = inputXml.replaceAll("\\r|\\n", "");
        GenericData.Record datum = AvroTools.xmlToGenericRecord(inputXml, schema);

        KafkaProducer<String, Object> producer = new KafkaProducer<String, Object>(prodProps);
        ProducerRecord<String, Object> avroRecord = new ProducerRecord<String, Object>("Rtt", null, datum);

        producer.send(avroRecord);
        producer.close();

    }
}
