package lu.pistache.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.naming.OperationNotSupportedException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.charset.Charset;

public class AvroTools {


    private static Document createDomDocument(String inputXml) throws ParserConfigurationException, SAXException, IOException {
        InputStream inputStream = new ByteArrayInputStream(inputXml.getBytes(Charset.forName("UTF-8")));
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        return builder.parse(inputStream);
    }

    public static boolean validate(String schema, String xml) throws SchemaValidationException {
        try {
            Schema schemaFromString = AvroTools.createSchemaFromString(schema);
            GenericData.Record datum = AvroTools.domNodeToGenericRecord(createDomDocument(xml), schemaFromString);
            return GenericData.get().validate(schemaFromString, datum);
        } catch (ParserConfigurationException e) {
            throw new SchemaValidationException("Can't parse XML", e);
        } catch (SAXException e) {
            throw new SchemaValidationException("Can't parse XML", e);
        } catch (IOException e) {
            throw new SchemaValidationException("Can't parse XML", e);
        } catch (OperationNotSupportedException e) {
            throw new SchemaValidationException("Can't parse XML", e);
        }
    }

    public static boolean validate(Schema schema, GenericData.Record datum) {
        return GenericData.get().validate(schema, datum);
    }


    public static GenericData.Record xmlToGenericRecord(String xml, Schema schema) throws IOException, SAXException, ParserConfigurationException, SchemaValidationException, OperationNotSupportedException {
        Document domDocument = AvroTools.createDomDocument(xml);
        return domNodeToGenericRecord(domDocument.getDocumentElement(), schema);

    }

    protected static GenericData.Record domNodeToGenericRecord(Node el, Schema schema) throws OperationNotSupportedException, SchemaValidationException {
        GenericData.Record record = new GenericData.Record(schema);


        NodeList childNodes = el.getChildNodes();
        //for all childnodes
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            NodeList nodeChildNodes = node.getChildNodes();
            for (int j = 0; j < nodeChildNodes.getLength(); j++) {
                if (nodeChildNodes.item(j).getNodeType() == Node.TEXT_NODE) {
                    //it's the value of the node. set the value
                    if (!nodeExists(schema, node)) throw new SchemaValidationException("Node: " + node.getNodeName() + " not found in schema: " + schema.getName());
                    Schema.Type filedType = getFiledType(schema, node);
                    String nodeTextContent = nodeChildNodes.item(j).getTextContent();
                    record.put(node.getNodeName(), convertedNodeValue(nodeTextContent, filedType));
                } else if (node.getNodeType() == Node.ELEMENT_NODE) {
                    //it's a child node. recursive call
                    GenericData.Record innerRecord = domNodeToGenericRecord(node, schema.getField(node.getNodeName()).schema());
                    record.put(node.getNodeName(), innerRecord);
                } else {
                    throw new RuntimeException("The XML structure is not recognised. Got element type " + node.getNodeType());
                }

            }
        }

        return record;
    }

    public static Schema createSchemaFromString(String avroSchemaString) {
        return new Schema.Parser().parse(avroSchemaString);
    }

    private static boolean nodeExists(Schema schema, Node node) {
        return schema.getField(node.getNodeName()) != null;
    }

    private static Schema.Type getFiledType(Schema schema, Node node) {
        return schema.getField(node.getNodeName()).schema().getType();

    }

    private static Object convertedNodeValue(String value, Schema.Type type) throws OperationNotSupportedException {
        switch (type) {
            case STRING:
                return value;
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case NULL:
                return null;
            default:
                throw new OperationNotSupportedException("Type not supported yet");
        }
    }
}
