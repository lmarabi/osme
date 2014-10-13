/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package osm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author turtle
 */
public class Build {

    public static void main(String[] args) throws IOException {
        File file = new File(System.getProperty("user.dir") + "/Data/HadoopTest300.osm");
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        String text = "";
        int i = 1;
        boolean withTag = false;
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        writer.write("<osm version=\"0.6\" generator=\"CGImap 0.0.2\" copyright=\"OpenStreetMap and contributors\" attribution=\"http://www.openstreetmap.org/copyright\" license=\"http://opendatacommons.org/licenses/odbl/1-0/\">");
        writer.write("\n<bounds minlat=\"21.3940000\" minlon=\"39.7830000\" maxlat=\"21.4570000\" maxlon=\"39.8530000\">");
        writer.write("\n</bounds>\n");
        while (megaByte(file.length()) < 250) {
            if (withTag) {
                text = "<node id=\"" + i + "\" lat=\"21.4221823\" lon=\"39.8331833\" user=\"Claudius Henrichs\" uid=\"18069\" visible=\"true\" version=\"4\" changeset=\"13664242\" timestamp=\"2012-10-28T17:12:52Z\">\n"
                        + "<tag k=\"highway\" v=\"motorway_junction\"/>\n</node>";
                withTag = false;
            } else {
                text = "<node id=\"" + i + "\" lat=\"21.4221823\" lon=\"39.8331833\" user=\"Claudius Henrichs\" uid=\"18069\" visible=\"true\" version=\"4\" changeset=\"13664242\" timestamp=\"2011-11-02T02:10:10Z\"/>";
                withTag = true;
            }
            
            i++;
            writer.write(text);
            writer.newLine();
        }
        
        i = 1;
        withTag = false;
        while (megaByte(file.length()) < 270) {
            
            if (withTag) {
                text = " <way id=\"" + i + "\" user=\"Alex Rollin\" uid=\"538459\" visible=\"true\" version=\"1\" changeset=\"13623423\" timestamp=\"2012-10-25T05:14:12Z\">\n"
                        + "<nd ref=\"1\"/>\n"
                        + "<nd ref=\"2\"/>\n"
                        + "<nd ref=\"3\"/>\n"
                        + "<nd ref=\"4\"/>\n"
                        + "<nd ref=\"5\"/>\n"
                        + "<nd ref=\"6\"/>\n"
                        + "<nd ref=\"7\"/>\n"
                        + "<nd ref=\"8\"/>\n"
                        + "<nd ref=\"9\"/>\n"
                        + "<tag k=\"water\" v=\"residential\"/>\n"
                        + "<tag k=\"building\" v=\"residential\"/>\n"
                        + "<tag k=\"test\" v=\"residential\"/>\n"
                        + "<tag k=\"border\" v=\"residential\"/>\n"
                        + "<tag k=\"countery\" v=\"residential\"/>\n"
                        + "<tag k=\"highway\" v=\"residential\"/>\n</way>";
                withTag = false;
            } else {
                text = " <way id=\"" + i + "\" user=\"Alex Rollin\" uid=\"538459\" visible=\"true\" version=\"1\" changeset=\"13623423\" timestamp=\"2012-10-25T05:14:12Z\">\n"
                        + "<nd ref=\"1\"/>\n"
                        + "<nd ref=\"2\"/>\n"
                        + "<nd ref=\"3\"/>\n"
                        + "<nd ref=\"4\"/>\n"
                        + "<nd ref=\"5\"/>\n"
                        + "<nd ref=\"6\"/>\n"
                        + "<nd ref=\"7\"/>\n"
                        + "<nd ref=\"8\"/>\n"
                        + "<nd ref=\"9\"/>" + "\n</way>";
                withTag = true;
            }
            
            i++;
            writer.write(text);
            writer.newLine();
            
        }
        
        i = 1;
        withTag = false;
        while (megaByte(file.length()) < 300) {
            if (withTag) {
                text = " <relation id=\"" + i + "\" user=\"Moayad Al-Jishi\" uid=\"793391\" visible=\"true\" version=\"5\" changeset=\"12673670\" timestamp=\"2012-08-09T20:49:56Z\">\n"
                        + "<member type=\"way\" ref=\"3\" role=\"outer\"/>\n"
                        + "<member type=\"way\" ref=\"6\" role=\"inner\"/>\n"
                        + "<tag k=\"amenity\" v=\"place_of_worship\"/>\n"
                        + "<tag k=\"building\" v=\"mosque\"/>\n"
                        + "<tag k=\"height\" v=\"15\"/>\n"
                        + "<tag k=\"int_name\" v=\"Al Masjid al Haram\"/>\n"
                        + "<tag k=\"is_in\" v=\"Saudi Arabia , المملكة العربية السعودية\"/>\n"
                        + "<tag k=\"name\" v=\"المسجد الحرام\"/>\n"
                        + "<tag k=\"name:ar\" v=\"المسجد الحرام\"/>\n"
                        + "<tag k=\"name:de\" v=\"al-Haram-Moschee\"/>\n"
                        + "<tag k=\"name:en\" v=\"Al Masjid al Haram\"/>\n</relation>";
                withTag = false;
            } else {
                text = " <relation id=\"" + i + "\" user=\"Moayad Al-Jishi\" uid=\"793391\" visible=\"true\" version=\"5\" changeset=\"12673670\" timestamp=\"2012-08-09T20:49:56Z\">\n"
                        + "<member type=\"way\" ref=\"3\" role=\"outer\"/>\n"
                        + "<member type=\"way\" ref=\"6\" role=\"inner\"/>\n"
                        + "</relation>";
                withTag = true;
            }
            i++;
            writer.write(text);
            writer.newLine();
        }
        writer.write("</osm>");
        writer.flush();
        writer.close();
        
    }
    
    public static int megaByte(long size) {
        return (int) (size / (1024 * 1024));
    }
}
