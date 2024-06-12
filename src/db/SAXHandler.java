package db;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;
class SAXHandler extends DefaultHandler {
    private List<Publication> publications;
    private Publication currentPublication;
    private StringBuilder content;

    public SAXHandler(List<Publication> publications) {
        this.publications = publications;
        this.content = new StringBuilder();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes){
        if (qName.equalsIgnoreCase("article") || qName.equalsIgnoreCase("inproceedings") || qName.equalsIgnoreCase("incollection")) {
            String[] parts = attributes.getValue(1).split("/");
            String venue;
            String article;
            if (parts[0].equals("journals") && parts[1].equals("pacmmod")) {
                venue = "sigmod";
            } else if (parts[0].equals("journals") && parts[1].equals("pvldb")) {
                venue = "vldb";
            } else {
                venue = attributes.getValue(1).split("/")[1];
            }
            article = attributes.getValue(1).split("/")[2];

            switch(qName.toLowerCase()) {
                case "article", "inproceedings", "incollection":
                    currentPublication = new Publication(attributes.getValue("key"), venue, article);
                    currentPublication.type = qName;
                    break;
            }
        }
        content.setLength(0); // Clear content buffer
    }

    public void endElement(String uri, String localName, String qName) {
        if (currentPublication != null) {
            switch (qName.toLowerCase()) {
                case "author":
                    if (currentPublication.author == null) {
                        currentPublication.author = content.toString();
                    } else {
                        currentPublication.author += ", " + content.toString();
                    }
                    break;
                case "booktitle":
                    currentPublication.booktitle = content.toString();
                    break;
                case "crossref":
                    currentPublication.crossref = content.toString();
                case "ee":
                    if (currentPublication.ee == null) {
                        currentPublication.ee = content.toString();
                    } else {
                        currentPublication.ee += ", " + content.toString();
                    }
                    break;
                case "journal":
                    currentPublication.journal = content.toString();
                    break;
                case "number":
                    currentPublication.number = content.toString();
                    break;
                case "pages":
                    currentPublication.pages = content.toString();
                    break;
                case "title":
                    currentPublication.title = content.toString();
                    break;
                case "url":
                    currentPublication.url = content.toString();
                    break;
                case "volume":
                    currentPublication.volume = content.toString();
                    break;
                case "year":
                    currentPublication.year = content.toString();
                    currentPublication.venue_year = currentPublication.venue + "_" + content.toString();
                    break;

                case "article", "inproceedings", "incollection":
                    publications.add(currentPublication);
                    break;
            }
        }
    }

    public void characters(char[] ch, int start, int length) {
        content.append(new String(ch, start, length));
    }
}

