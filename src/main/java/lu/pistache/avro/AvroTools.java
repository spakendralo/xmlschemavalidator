package lu.pistache.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class AvroTools {


    private static Document createDomDocument(String inputXml) throws ParserConfigurationException, SAXException, IOException {
        InputStream inputStream = new ByteArrayInputStream(inputXml.getBytes(Charset.forName("UTF-8")));
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        return builder.parse(inputStream);
    }

    public static boolean validate(String schema, String xml) throws SchemaValidationException {
        try {
            Schema schemaFromString = AvroTools.createSchemaFromString(schema);
            GenericData.Record datum = AvroTools.domNodeToGenericRecord(createDomDocument(xml).getDocumentElement(), schemaFromString);
            return GenericData.get().validate(schemaFromString, datum);
        } catch (ParserConfigurationException e) {
            throw new SchemaValidationException("Can't validate XML", e);
        } catch (SAXException e) {
            throw new SchemaValidationException("Can't validate XML", e);
        } catch (IOException e) {
            throw new SchemaValidationException("Can't validate XML", e);
        } catch (ParseXmlException e) {
            throw new SchemaValidationException("Can't validate XML", e);
        } catch (AvroObjectCreationException e) {
            throw new SchemaValidationException("Can't create AVRO object", e);
        }
    }

    public static boolean validate(Schema schema, GenericData.Record datum) {
        return GenericData.get().validate(schema, datum);
    }


    public static GenericData.Record xmlToGenericRecord(String xml, Schema schema) throws AvroObjectCreationException, ParseXmlException {
        Document domDocument = null;
        try {
            domDocument = AvroTools.createDomDocument(xml);
        } catch (SAXException e) {
            throw new ParseXmlException("Can't parse XML", e);
        } catch (IOException e) {
            throw new ParseXmlException("Can't parse XML", e);
        } catch (ParserConfigurationException e) {
            throw new ParseXmlException("Can't parse XML", e);
        }

        //remove empty lines which are otherwise valid DOM nodes
        try {
            removeEmptyLinesFromDomDocument(domDocument);
        } catch (XPathExpressionException e) {
            throw new ParseXmlException("Can't correctly clean the XML file", e);
        }

        return domNodeToGenericRecord(domDocument.getDocumentElement(), schema);
    }

    private static void removeEmptyLinesFromDomDocument(Document domDocument) throws XPathExpressionException {
        XPath xp = XPathFactory.newInstance().newXPath();
        NodeList nl = (NodeList) xp.evaluate("//text()[normalize-space(.)='']", domDocument, XPathConstants.NODESET);

        for (int i=0; i < nl.getLength(); ++i) {
            Node node = nl.item(i);
            node.getParentNode().removeChild(node);
        }
    }

    protected static GenericData.Record domNodeToGenericRecord(Node el, Schema schema) throws ParseXmlException, AvroObjectCreationException {
        //If this is a union, then only accept something like union {null, record} and pull out the first record
        if (schema.getType() == Schema.Type.UNION) {
            schema = replaceUnionSchemaWithEmbeddedRecordSchema(schema);
        }
        GenericData.Record record = new GenericData.Record(schema);


        NodeList childNodes = el.getChildNodes();
        //for all childnodes
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            NodeList nodeChildNodes = node.getChildNodes();
            for (int j = 0; j < nodeChildNodes.getLength(); j++) {
                if (nodeChildNodes.item(j).getNodeType() == Node.TEXT_NODE) {
                    //it's the value of the node. set the value
                    if (!nodeExists(schema, node)) throw new ParseXmlException("Node \"" + node.getNodeName() + "\" not found in schema \"" + schema.getName() + "\"");
                    record.put(node.getNodeName(), convertedNodeValue(node, nodeChildNodes.item(j), schema));
                } else if (node.getNodeType() == Node.ELEMENT_NODE) {
                    //it's a child node. recursive call
                    GenericData.Record innerRecord = domNodeToGenericRecord(node, schema.getField(node.getNodeName()).schema());
                    record.put(node.getNodeName(), innerRecord);
                } else {
                    throw new ParseXmlException("The XML structure is not recognised. Got element type " + node.getNodeType());
                }

            }
        }

        return record;
    }

    /**
     * Searches the union and extracts the first RECORD it finds
     * @param schema
     * @return
     * @throws AvroObjectCreationException
     */
    private static Schema replaceUnionSchemaWithEmbeddedRecordSchema(Schema schema) throws AvroObjectCreationException {
        boolean foundARecord = false;
        List<Schema> schemaTypes = schema.getTypes();
        for (Schema schemaForType : schemaTypes) {
            if (schemaForType.getType() == Schema.Type.RECORD) {
                foundARecord = true;
                schema = schemaForType;
                break;
            }
        }
        if (!foundARecord) {
            List<Schema.Type> unionTypes = getUnionTypes(schemaTypes);
            throw new AvroObjectCreationException("Unsupported UNION at this point: " + unionTypes.toString() + ". At this step the supported unions should contain a record. This could be a problem in the algorithm itself, not the entry data.");
        }
        return schema;
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

    private static Object convertedNodeValue(Node parentNode, Node node, Schema schema) throws AvroObjectCreationException {
        Schema.Type filedType = getFiledType(schema, parentNode);
        String nodeTextContent = node.getTextContent();
        switch (filedType) {
            case STRING:
                return getStringValue(nodeTextContent);
            case INT:
                return getIntValue(nodeTextContent);
            case LONG:
                return getLongValue(nodeTextContent);
            case BOOLEAN:
                return getBooleanValue(nodeTextContent);
            case NULL:
                return null;
            case UNION:
                List<Schema.Type> types = getUnionTypes(parentNode, schema);
                //Return a value of a type in this order of preference
                if (types.contains(Schema.Type.STRING)) {
                    return getStringValue(nodeTextContent);
                }
                else if (types.contains(Schema.Type.INT)) {
                    return getIntValue(nodeTextContent);
                }
                else if (types.contains(Schema.Type.LONG)) {
                    return getLongValue(nodeTextContent);
                }
                else if (types.contains(Schema.Type.BOOLEAN)) {
                    return getBooleanValue(nodeTextContent);
                }
                else {
                    //Should you get a union like [null, RECORD], than this is a problem in the algorithm. It did not
                    //match correctly an DOM node with an AVRO element. Something like: it took empty lines as a DOM node.
                    //Otherwise, it's just not implemented yet.
                    throw new AvroObjectCreationException("Unsupported UNION type:" + types.toString());
                }
            default:
                throw new AvroObjectCreationException("Type not supported: " + filedType);
        }
    }

    private static boolean getBooleanValue(String nodeTextContent) {
        return Boolean.parseBoolean(nodeTextContent);
    }

    private static long getLongValue(String nodeTextContent) {
        return Long.parseLong(nodeTextContent);
    }

    private static int getIntValue(String nodeTextContent) {
        return Integer.parseInt(nodeTextContent);
    }

    private static String getStringValue(String nodeTextContent) {
        return nodeTextContent;
    }

    private static List<Schema.Type> getUnionTypes(Node parentNode, Schema schema) {
        List<Schema> schemas = schema.getField(parentNode.getNodeName()).schema().getTypes(); //normally empty Schema objects, just the Type is populated
        List<Schema.Type> types = new ArrayList<>();
        for (Schema typeSchema : schemas) {
            types.add(typeSchema.getType());
        }
        return types;
    }

    private static List<Schema.Type> getUnionTypes(List<Schema> schemas) {
        List<Schema.Type> types = new ArrayList<>();
        for (Schema typeSchema : schemas) {
            types.add(typeSchema.getType());
        }
        return types;
    }
}
