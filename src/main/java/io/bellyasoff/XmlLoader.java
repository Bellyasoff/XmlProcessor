package io.bellyasoff;

import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.XMLReader;

/**
 * @author IlyaB
 */
public class XmlLoader {

    public GPathResult loadXml(String xmlUrl) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(false);

            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

            XMLReader reader = factory.newSAXParser().getXMLReader();

            return new XmlSlurper(reader).parse(xmlUrl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load XML", e);
        }
    }
}
