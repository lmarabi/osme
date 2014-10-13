/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package osm;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author turtle
 */
public class OsmExtractor extends Configured implements Tool {

    /**
     * Counts the words in each line. For each line of input, break the line
     * into words and emit them as (<b>word</b>, <b>1</b>).
     */
    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, IntWritable, Text> {

        private final static IntWritable outputKey = new IntWritable();

        /**
         * Properties that used to check for the all possible key For the Road
         * Network data.
         */
        public enum RoadKey {

            highway, junction, ford, route, cutting, tunnel, amenity;

            public static boolean isRoadFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    RoadKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        /**
         * This is to values of all possible tag values related to Road Network
         */
        public enum RoadValue {

            yes, street, highway, service, parking_aisle, motorway, motorway_link, trunk,
            trunk_link, primary, primary_link, secondary, secondary_link, tertiary,
            tertiary_link, living_street, residential, unclassified, track, road,
            roundabout, escape, mini_roundabout, motorway_junction, passing_place,
            rest_area, turning_circle, detour, parking_entrance;

            public static boolean isRoadValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    RoadValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        // Properties that used to check for the tags
        public enum LakeKey {

            natural, waterway;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    LakeKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum LakeValue {

            bay, wetland, water, coastline, riverbank, dock, boatyard;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    LakeValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum SportKey {

            leisure, landuse;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    SportKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum SportValue {

            sports_centre, stadium, track, pitch, golf_course, water_park,
            swimming_pool, recreation_ground, piste;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    SportValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum RiverKey {

            waterway;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    RiverKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum RiverValue {

            river, stream, ditch, dam, wadi, canal, drain;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    RiverValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ParkKey {

            leisure, boundary, landuse, natural;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ParkKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ParkValue {

            wood, tree_row, grassland, park, golf_course, national_park, garden,
            nature_reserve, forest, grass, tree, orchard, farmland, protected_area;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ParkValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum BorderKey {

            boundary;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum BorderValue {

            administrative, postal_code, maritime, political;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum PoliticalKey {

            boundary;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum PoliticalValue {

            political;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum MaritimeKey {

            boundary;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum MaritimeValue {

            maritime;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum PostalKey {

            boundary;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum PostalValue {

            postal_code;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum AdministrativeKey {

            boundary;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum AdministrativeValue {

            administrative;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BorderValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ResidentKey {

            building;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ResidentKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ResidentValue {

            apartments, dormitory, hotel, house, residential, terrace;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ResidentValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum CommerceKey {

            building, landuse;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    CommerceKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum CommerceValue {

            commercial, industrial, retail, warehouse;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    CommerceValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum WorshipKey {

            amenity, building;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    WorshipKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum WorshipValue {

            place_of_worship, cathedral, chapel, church;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    WorshipValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ServicesKey {

            amenity, building;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ServicesKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum ServicesValue {

            hospital, civic;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    ServicesValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum EducationKey {

            amenity, building;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    EducationKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum EducationValue {

            school, university;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    EducationValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum BuildingKey {

            amenity, building;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BuildingKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum BuildingValue {

            yes;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    BuildingValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum CemeteryKey {

            landuse;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    CemeteryKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum CemeteryValue {

            cemetery;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    CemeteryValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum DesertKey {

            natural;

            public static boolean isFlag(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    DesertKey.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum DesertValue {

            sand, peak, heath, desert;

            public static boolean isValue(String pvalue) {
                //Here inside try we check whether the pvalue is in RoadKey eunm, or not
                try {
                    DesertValue.valueOf(pvalue);
                    return true;
                } catch (Exception e) {
                    // if not then the class enum will fire an exception
                }
                return false;
            }
        }

        public enum fileTag {

            node, way, relation, node_tags, way_nodes, way_tags, relation_tags,
            member_relation_ways, member_relation_nodes, member_relation_relations,
            road_edges, lake_edges, park_edges, river_edges, building_edges,
            cemetery_edges, sport_edges, border_edges, resident_edges,
            commerce_edges, worship_edges, services_edges, education_edges,
            desert_edges, region_edges, district_edges,
            city_edges, neighborhood_edges, administrative_edges, postal_edges,
            maritime_edges, political_edges, national_edges;

            public static fileTag enumOf(int v) {
                switch (v) {
                    case 0:
                        return node;
                    case 1:
                        return way;
                    case 2:
                        return relation;
                    case 3:
                        return node_tags;
                    case 4:
                        return way_nodes;
                    case 5:
                        return way_tags;
                    case 6:
                        return relation_tags;
                    case 7:
                        return member_relation_ways;
                    case 8:
                        return member_relation_nodes;
                    case 9:
                        return member_relation_relations;
                    case 10:
                        return road_edges;
                    case 11:
                        return lake_edges;
                    case 12:
                        return park_edges;
                    case 13:
                        return river_edges;
                    case 14:
                        return building_edges;
                    case 15:
                        return cemetery_edges;
                    case 16:
                        return sport_edges;
                    case 17:
                        return border_edges;
                    case 18:
                        return resident_edges;
                    case 19:
                        return commerce_edges;
                    case 20:
                        return worship_edges;
                    case 21:
                        return services_edges;
                    case 22:
                        return education_edges;
                    case 23:
                        return desert_edges;
                    case 24:
                        return region_edges;
                    case 25:
                        return district_edges;
                    case 26:
                        return city_edges;
                    case 27:
                        return neighborhood_edges;
                    case 28:
                        return administrative_edges;
                    case 29:
                        return postal_edges;
                    case 30:
                        return maritime_edges;
                    case 31:
                        return political_edges;
                    case 32:
                        return national_edges;
                    default:
                        break;
                }
                return null;
            }
        }
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                OutputCollector<IntWritable, Text> output,
                Reporter reporter) throws IOException {
            String line = "";
            String id;
            fileTag flag;
            List<fileTag> table = new ArrayList<fileTag>();
            //1List<fileTag> table = new ArrayList<>();
            try {
                DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                InputSource is = new InputSource();
                is.setCharacterStream(new StringReader(value.toString()));

                Document doc = db.parse(is);
                //NodeList nodes = doc.getElementsByTagName(doc.getDocumentElement().getNodeName());
                Element element = doc.getDocumentElement();

                if(element.getTagName().toString().equals("node")) {
                        flag = fileTag.node;
                        outputKey.set(flag.ordinal());
                        id = element.getAttribute("id");
                        line = id + "," + element.getAttribute("lat") + ","
                                + element.getAttribute("lon");
                        word.set(line);
                        output.collect(outputKey, word);
                        if (element.hasChildNodes()) {
                            flag = fileTag.node_tags;
                            outputKey.set(flag.ordinal());
                            NodeList tags = element.getElementsByTagName("tag");
                            for (int i = 0; i < tags.getLength(); i++) {
                                Node tagNode = tags.item(i);
                                Element tag = (Element) tagNode;
                                line = id;
                                line += ",{"+"\"" + 
                                        tag.getAttribute("k").replace(",", "-") 
                                        + "\"=\"" 
                                        + tag.getAttribute("v").replace(",", "-") 
                                        + "\""+"}";
                                word.set(line);
                                output.collect(outputKey, word);
                            }
                        }
                }else if(element.getTagName().toString().equals("way")){
                        // Extract way Table
                        flag = fileTag.way;
                        outputKey.set(flag.ordinal());
                        id = element.getAttribute("id");
                        line = id;
                        word.set(line);
                        output.collect(outputKey, word);
                        if (element.hasChildNodes()) {
                            NodeList refs = element.getElementsByTagName("nd");
                            NodeList tags = element.getElementsByTagName("tag");
                            // Extract way_nodes table
                            if (refs.getLength() > 0) {
                                flag = fileTag.way_nodes;
                                outputKey.set(flag.ordinal());
                                for (int i = 0; i < refs.getLength(); i++) {
                                    line = id;
                                    Node refNode = refs.item(i);
                                    Element ref = (Element) refNode;
                                    line += "," + ref.getAttribute("ref") + "," + i;
                                    word.set(line);
                                    output.collect(outputKey, word);
                                }
                            }
                            // Extract way_tags table 
                            if (tags.getLength() > 0) {
                                flag = fileTag.way_tags;
                                StringBuilder tagColumn = new StringBuilder();
                                outputKey.set(flag.ordinal());
                                for (int i = 0; i < tags.getLength(); i++) {
                                    line = id;
                                    Node tagNode = tags.item(i);
                                    Element tag = (Element) tagNode;
                                    tagColumn.append("{" + "\"" + tag.getAttribute("k").replace(",", "-") + "\"=\"" + tag.getAttribute("v").replace(",", "-") + "\"" +"}");
                                    line += ",{" + "\"" + tag.getAttribute("k").replace(",", "-") + "\"=\"" + tag.getAttribute("v").replace(",", "-") + "\"" +"}";
                                    word.set(line);
                                    output.collect(outputKey, word);
                                    //Here extract on the fly
                                    if (RoadKey.isRoadFlag(tag.getAttribute("k"))) {
                                        if (RoadValue.isRoadValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.road_edges)) {
                                                table.add(fileTag.road_edges);
                                            }
                                        }
                                    }
                                    if (LakeKey.isFlag(tag.getAttribute("k"))) {
                                        if (LakeValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.lake_edges)) {
                                                table.add(fileTag.lake_edges);
                                            }
                                        }
                                    }
                                    if (ParkKey.isFlag(tag.getAttribute("k"))) {
                                        if (ParkValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.park_edges)) {
                                                table.add(fileTag.park_edges);
                                            }
                                        }
                                    }
                                    if (RiverKey.isFlag(tag.getAttribute("k"))) {
                                        if (RiverValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.river_edges)) {
                                                table.add(fileTag.river_edges);
                                            }
                                        }
                                    }
                                    if (BuildingKey.isFlag(tag.getAttribute("k"))) {
                                        if (BuildingValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.building_edges)) {
                                                table.add(fileTag.building_edges);
                                            }
                                        }
                                    }
                                    if (CemeteryKey.isFlag(tag.getAttribute("k"))) {
                                        if (CemeteryValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.cemetery_edges)) {
                                                table.add(fileTag.cemetery_edges);
                                            }
                                        }
                                    }
                                    if (SportKey.isFlag(tag.getAttribute("k"))) {
                                        if (SportValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.sport_edges)) {
                                                table.add(fileTag.sport_edges);
                                            }
                                        }
                                    }
                                    if (BorderKey.isFlag(tag.getAttribute("k"))) {
                                        if (BorderValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.border_edges)) {
                                                table.add(fileTag.border_edges);
                                            }
                                        }
                                    }
                                    if (ResidentKey.isFlag(tag.getAttribute("k"))) {
                                        if (ResidentValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.resident_edges)) {
                                                table.add(fileTag.resident_edges);
                                            }
                                        }
                                    }
                                    if (CommerceKey.isFlag(tag.getAttribute("k"))) {
                                        if (CommerceValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.commerce_edges)) {
                                                table.add(fileTag.commerce_edges);
                                            }
                                        }
                                    }
                                    if (WorshipKey.isFlag(tag.getAttribute("k"))) {
                                        if (WorshipValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.worship_edges)) {
                                                table.add(fileTag.worship_edges);
                                            }
                                        }
                                    }
                                    if (ServicesKey.isFlag(tag.getAttribute("k"))) {
                                        if (ServicesValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.services_edges)) {
                                                table.add(fileTag.services_edges);
                                            }
                                        }
                                    }
                                    if (EducationKey.isFlag(tag.getAttribute("k"))) {
                                        if (EducationValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.education_edges)) {
                                                table.add(fileTag.education_edges);
                                            }
                                        }
                                    }
                                    if (DesertKey.isFlag(tag.getAttribute("k"))) {
                                        if (DesertValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.desert_edges)) {
                                                table.add(fileTag.desert_edges);
                                            }
                                        }
                                    }
                                    if (AdministrativeKey.isFlag(tag.getAttribute("k"))) {
                                        if (AdministrativeValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.administrative_edges)) {
                                                table.add(fileTag.administrative_edges);
                                            }
                                        }
                                    }
                                    if (PostalKey.isFlag(tag.getAttribute("k"))) {
                                        if (PostalValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.postal_edges)) {
                                                table.add(fileTag.postal_edges);
                                            }
                                        }
                                    }
                                    if (MaritimeKey.isFlag(tag.getAttribute("k"))) {
                                        if (MaritimeValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.maritime_edges)) {
                                                table.add(fileTag.maritime_edges);
                                            }
                                        }
                                    }
                                    if (PoliticalKey.isFlag(tag.getAttribute("k"))) {
                                        if (PoliticalValue.isValue(tag.getAttribute("v"))) {
                                            if (!table.contains(fileTag.political_edges)) {
                                                table.add(fileTag.political_edges);
                                            }
                                        }
                                    }
                                    if (tag.getAttribute("k").equals("admin_level")) {
                                        if (tag.getAttribute("v").equals("1")
                                                || tag.getAttribute("v").equals("2")) {
                                            if (!table.contains(fileTag.national_edges)) {
                                                table.add(fileTag.national_edges);
                                            }
                                        }
                                        if (tag.getAttribute("v").equals("3")
                                                || tag.getAttribute("v").equals("4")) {
                                            if (!table.contains(fileTag.region_edges)) {
                                                table.add(fileTag.region_edges);
                                            }
                                        }
                                        if (tag.getAttribute("v").equals("5")
                                                || tag.getAttribute("v").equals("6")) {
                                            if (!table.contains(fileTag.district_edges)) {
                                                table.add(fileTag.district_edges);
                                            }
                                        }
                                        if (tag.getAttribute("v").equals("7")
                                                || tag.getAttribute("v").equals("8")) {
                                            if (!table.contains(fileTag.city_edges)) {
                                                table.add(fileTag.city_edges);
                                            }
                                        }
                                        if (tag.getAttribute("v").equals("9")
                                                || tag.getAttribute("v").equals("10")) {
                                            if (!table.contains(fileTag.neighborhood_edges)) {
                                                table.add(fileTag.neighborhood_edges);
                                            }
                                        }
                                    }
                                }

                                // Extract data if tags exist. 
                                if (!table.isEmpty()) {
                                    // check if there is nodes inside the way
                                    if (refs.getLength() > 0) {
                                        String start = null;
                                        String end = null;
                                        for (int i = 0; i < refs.getLength(); i++) {
                                            Node refNode = refs.item(i);
                                            Element ref = (Element) refNode;
                                            String node = ref.getAttribute("ref");
                                            if (start == null) {
                                                start = node;
                                            } else {
                                                end = node;
                                                line = "0,"+start 
                                                        + "," 
                                                        + end 
                                                        + "," 
                                                        + id + ","
                                                        +tagColumn.toString();
                                                word.set(line);
                                                for (fileTag type : table) {
                                                    outputKey.set(type.ordinal());
                                                    output.collect(outputKey, word);
                                                }
                                                start = end;
                                                end = null;
                                            }
                                        }
                                    }
                                }


                            }


                        }
                }else if(element.getTagName().toString().equals("relation")){
                        flag = fileTag.relation;
                        outputKey.set(flag.ordinal());
                        id = element.getAttribute("id");
                        word.set(id);
                        output.collect(outputKey, word);
                        if (element.hasChildNodes()) {
                            NodeList members = element.getElementsByTagName("member");
                            NodeList tags = element.getElementsByTagName("tag");
                            if (members.getLength() > 0) {
                                for (int i = 0; i < members.getLength(); i++) {
                                    line = id;
                                    Node refNode = members.item(i);
                                    Element member = (Element) refNode;
                                    line += "," + member.getAttribute("ref")
                                            + "," + member.getAttribute("role")
                                            + "," + i;
                                    if(member.getAttribute("type").toString().equals("node")){
                                        flag = fileTag.member_relation_nodes;
                                    }else if(member.getAttribute("type").toString().equals("way")){
                                        flag = fileTag.member_relation_ways;
                                    }else if(member.getAttribute("type").toString().equals("relation")){
                                        flag = fileTag.member_relation_relations;
                                    }else{
                                    }
                                    word.set(line);
                                    outputKey.set(flag.ordinal());
                                    output.collect(outputKey, word);
                                }
                            }
                            if (tags.getLength() > 0) {
                                flag = fileTag.relation_tags;
                                outputKey.set(flag.ordinal());
                                for (int i = 0; i < tags.getLength(); i++) {
                                    line = id;
                                    Node tagNode = tags.item(i);
                                    Element tag = (Element) tagNode;
                                    line += ",{" + "\"" + tag.getAttribute("k").replace(",", "-") + "\"=\"" + tag.getAttribute("v").replace(",", "-") + "\"" +"}";
                                    word.set(line);
                                    output.collect(outputKey, word);
                                }
                            }


                        }
                }
                

            } catch (ParserConfigurationException ex) {
                Logger.getLogger(OsmExtractor.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SAXException  ex) {
                Logger.getLogger(OsmExtractor.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     */
    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    static int printUsage() {
        System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    /**
     * The main driver for word count map/reduce program. Invoke this method to
     * submit the map/reduce job.
     *
     * @throws IOException When there is communication problems with the job
     * tracker.
     */
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), OsmExtractor.class);
        conf.setJobName("OsmExtractor");

        // the keys are words (strings)
        conf.setOutputKeyClass(IntWritable.class);
        // the values are counts (ints)
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(OsmExtractor.MapClass.class);
        conf.setNumReduceTasks(0);
//    conf.setCombinerClass(OsmExtractor.Reduce.class);

        //conf.setReducerClass(OsmExtractor.Reduce.class);
        conf.setInputFormat(OsmTextInputFormat.class);
        conf.setOutputFormat(OsmTextOutputFormat.class);
//    List<String> other_args = new ArrayList<String>();
//    for(int i=0; i < args.length; ++i) {
//      try {
//        if ("-m".equals(args[i])) {
//          conf.setNumMapTasks(Integer.parseInt(args[++i]));
//        } else if ("-r".equals(args[i])) {
//          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
//        } else {
//          other_args.add(args[i]);
//        }
//      } catch (NumberFormatException except) {
//        System.out.println("ERROR: Integer expected instead of " + args[i]);
//        return printUsage();
//      } catch (ArrayIndexOutOfBoundsException except) {
//        System.out.println("ERROR: Required parameter missing from " +
//                           args[i-1]);
//        return printUsage();
//      }
//    }
//    // Make sure there are exactly 2 parameters left.
//    if (other_args.size() != 2) {
//      System.out.println("ERROR: Wrong number of parameters: " +
//                         other_args.size() + " instead of 2.");
//      return printUsage();
//    }
        Path input = args.length > 0
                ? new Path(args[0])
                : new Path(System.getProperty("user.dir") + "/Data/Istanbul.osm");

        Path output = args.length > 1
                ? new Path(args[1])
                : new Path(System.getProperty("user.dir") + "/Data/hdfsoutput");
        FileSystem outfs = output.getFileSystem(conf);
        outfs.delete(output, true);
        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf, output);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: OsmExtractor <in> <out>");
//            System.exit(2);
//        }
//        Job job = new Job(conf, "OsmExtractor");
//        job.setJarByClass(OsmExtractor.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
//        job.setMapperClass(OsmExtractor.MapClass.class);
//        job.setNumReduceTasks(0);
//        job.setInputFormatClass(OsmTextInputFormat.class);
//        job.setOutputFormatClass(OsmTextOutputFormat.class);
//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        int res = ToolRunner.run(new Configuration(), new OsmExtractor(), args);
        System.exit(res);
    }
}
