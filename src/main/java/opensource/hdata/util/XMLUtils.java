package opensource.hdata.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.xml.sax.SAXException;

public class XMLUtils {

    /**
     * 加载XML文件
     * 
     * @param input
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static Element load(InputStream input) throws ParserConfigurationException, SAXException, IOException {
        DOMBuilder domBuilder = new DOMBuilder();
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = domBuilder.build(builder.parse(input));
        Element root = doc.getRootElement();
        return root;
    }

    public static Element load(String xmlpath) throws ParserConfigurationException, SAXException, IOException {
        FileInputStream fis = new FileInputStream(xmlpath);
        return load(fis);
    }
}
