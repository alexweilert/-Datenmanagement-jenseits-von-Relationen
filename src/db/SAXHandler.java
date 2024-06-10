package db;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.List;
class SAXHandler extends DefaultHandler {
    private List<Publication> publications;
    private Publication currentPublication;
    private StringBuilder content;

    public SAXHandler(List<Publication> publications) {
        this.publications = publications;
        this.content = new StringBuilder();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes){
        if (qName.equalsIgnoreCase("article") || qName.equalsIgnoreCase("inproceedings")) {
            currentPublication = new Publication();
            currentPublication.type = qName;
        }
        content.setLength(0); // Clear content buffer
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        if (currentPublication != null) {
            switch (qName.toLowerCase()) {
                case "title":
                    currentPublication.title = content.toString();
                    break;
                case "author":
                    if (currentPublication.author == null) {
                        currentPublication.author = content.toString();
                    } else {
                        currentPublication.author += ", " + content.toString();
                    }
                    break;
                case "pages":
                    currentPublication.pages = content.toString();
                    break;
                case "volume":
                    currentPublication.volume = content.toString();
                    break;
                case "journal":
                    currentPublication.journal = content.toString();
                    break;
                case "booktitle":
                    currentPublication.booktitle = content.toString();
                    break;
                case "number":
                    currentPublication.number = content.toString();
                    break;
                case "ee":
                    currentPublication.ee = content.toString();
                    break;
                case "url":
                    currentPublication.url = content.toString();
                    break;
                case "year":
                    currentPublication.year = content.toString();
                    break;
                case "article":
                case "inproceedings":
                    publications.add(currentPublication);
                    break;
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        content.append(new String(ch, start, length));
    }
}

