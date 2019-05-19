package lu.pistache.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.naming.OperationNotSupportedException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class TestSimpleXmlCheck {

    @Test
    public void simpleDatumCreation() throws ParserConfigurationException, IOException, SchemaValidationException, OperationNotSupportedException, SAXException {
        String avroSchemaString = Tools.readFile("src/test/resources/avsc/User.avsc");
        String inputXml = Tools.readFile("src/test/resources/input.xml");

        Schema schema = AvroTools.createSchemaFromString(avroSchemaString);

        GenericData.Record datum = AvroTools.xmlToGenericRecord(inputXml, schema);

        assert (datum != null);


    }

    @Test
    public void simpleXmlStringValidation() throws IOException, SchemaValidationException, OperationNotSupportedException, ParserConfigurationException, SAXException {
        String avroSchemaString = Tools.readFile("src/test/resources/avsc/User.avsc");
        String inputXml = Tools.readFile("src/test/resources/input.xml");

        boolean validation = AvroTools.validate(avroSchemaString, inputXml);
        assert validation;
    }
}
