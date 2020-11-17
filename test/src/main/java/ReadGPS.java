import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.format.gpx.GpxUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class ReadGPS {

  static XMLEventReader eventReader;
  static SimpleFeature nextFeature = null;
  static final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
  static final Stack<GPXDataElement> currentElementStack = new Stack<>();
  static GPXDataElement top = null;
  static double maxLength = Double.MAX_VALUE;
  protected static final SimpleFeatureType pointType = GpxUtils.createGPXPointDataType();
  private final static SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(pointType);

  public static void main(final String[] args) throws Exception {
    final File f = new File("F:\\gps\\csv", "Germany2.csv");
    Geometry germany = null;
    try (FileReader r =
        new FileReader(new File("D:\\development\\data\\ne_10m_admin_0_countries\\Germany.wkt"))) {
      germany = new WKTReader().read(r);
    }
    // final File f = new File("F:\\gps\\csv", "Malta.csv");
    final FileWriter out = new FileWriter(f);
    try (final CSVPrinter printer = CSVFormat.DEFAULT.withHeader("lat", "lon", "time").print(out)) {
      long i = 0;
      final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

      inputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
      final Collection<File> files =
          FileUtils.listFiles(
              new File("F:\\gps\\germany.tar\\germany\\gpx-planet-2013-04-09"),
              // new File("F:\\gps\\malta.tar\\malta\\gpx-planet-2013-04-09"),
              new SuffixFileFilter(".gpx"),
              DirectoryFileFilter.DIRECTORY);
      for (final File file : files) {
        currentElementStack.clear();
        top = new GPXDataElement("gpx", maxLength);
        eventReader = inputFactory.createXMLEventReader(new FileInputStream(file));
        init();
        if (!currentElementStack.isEmpty()) {
          nextFeature = getNext();
        }
        while (nextFeature != null) {

          nextFeature = getNext();
          if (nextFeature != null) {
            double lat = (Double) nextFeature.getAttribute("Latitude");
            double lon = (Double) nextFeature.getAttribute("Longitude");
            if (germany.intersects(
                GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat)))) {
              printer.printRecord(lat, lon, sdf.format(nextFeature.getAttribute("Timestamp")));
              if ((i % 1000000) == 0) {
                System.err.println("Scanned: " + i);
                printer.flush();
              }
              i++;
            }
          }
        }
      }
    }
  }

  private static void init() throws IOException, Exception {

    while (eventReader.hasNext()) {
      final XMLEvent event = eventReader.nextEvent();
      if (event.isStartElement()) {
        final StartElement node = event.asStartElement();
        if ("gpx".equals(node.getName().getLocalPart())) {
          currentElementStack.push(top);
          processElementAttributes(node, top);
          return;
        }
      }
    }
  }

  private static SimpleFeature getNext() throws Exception {

    GPXDataElement currentElement = currentElementStack.peek();
    SimpleFeature newFeature = null;
    while ((newFeature == null) && eventReader.hasNext()) {
      final XMLEvent event = eventReader.nextEvent();
      if (event.isStartElement()) {
        final StartElement node = event.asStartElement();
        if (!processElementValues(node, currentElement)) {
          final GPXDataElement newElement =
              new GPXDataElement(event.asStartElement().getName().getLocalPart(), maxLength);
          currentElement.addChild(newElement);
          currentElement = newElement;
          currentElementStack.push(currentElement);
          processElementAttributes(node, currentElement);
        }
      } else if (event.isEndElement()
          && event.asEndElement().getName().getLocalPart().equals(currentElement.elementType)) {
        final GPXDataElement child = currentElementStack.pop();
        newFeature = postProcess(child);
        if ((newFeature == null) && !currentElementStack.isEmpty()) {
          currentElement = currentElementStack.peek();
          // currentElement.removeChild(child);
        } else if (currentElementStack.size() == 1) {
          top.children.remove(child);
        }
      }
    }
    return newFeature;
  }

  private static SimpleFeature postProcess(final GPXDataElement element) {

    switch (element.elementType) {
      case "trkpt": {
        if (element.build(pointBuilder, null, false)) {
          if ((element.timestamp == null) || (element.lat == null) || (element.lon == null)) {
            pointBuilder.reset();
            return null;
          }
          return buildGeoWaveDataInstance(
              element.composeID("", false, true),
              GpxUtils.GPX_POINT_FEATURE,
              pointBuilder,
              null);
        }
        break;
      }
    }
    return null;
  }

  private static SimpleFeature buildGeoWaveDataInstance(
      final String id,
      final String key,
      final SimpleFeatureBuilder builder,
      final Map<String, String> additionalDataSet) {

    if (additionalDataSet != null) {
      for (final Map.Entry<String, String> entry : additionalDataSet.entrySet()) {
        builder.set(entry.getKey(), entry.getValue());
      }
    }
    return builder.buildFeature(id);
  }

  private static String getChildCharacters(final XMLEventReader eventReader, final String elType)
      throws Exception {
    final StringBuilder buf = new StringBuilder();
    XMLEvent event = eventReader.nextEvent();
    while (!(event.isEndElement()
        && event.asEndElement().getName().getLocalPart().equals(elType))) {
      if (event.isCharacters()) {
        buf.append(event.asCharacters().getData());
      }
      event = eventReader.nextEvent();
    }
    return buf.toString().trim();
  }

  private static boolean processElementValues(final StartElement node, final GPXDataElement element)
      throws Exception {
    switch (node.getName().getLocalPart()) {
      case "ele": {
        element.elevation = Double.parseDouble(getChildCharacters(eventReader, "ele"));
        break;
      }
      case "magvar": {
        element.magvar = Double.parseDouble(getChildCharacters(eventReader, "magvar"));
        break;
      }
      case "geoidheight": {
        element.geoidheight = Double.parseDouble(getChildCharacters(eventReader, "geoidheight"));
        break;
      }
      case "name": {
        element.name = getChildCharacters(eventReader, "name");
        break;
      }
      case "cmt": {
        element.cmt = getChildCharacters(eventReader, "cmt");
        break;
      }
      case "desc": {
        element.desc = getChildCharacters(eventReader, "desc");
        break;
      }
      case "src": {
        element.src = getChildCharacters(eventReader, "src");
        break;
      }
      case "link": {
        element.link = getChildCharacters(eventReader, "link");
        break;
      }
      case "sym": {
        element.sym = getChildCharacters(eventReader, "sym");
        break;
      }
      case "type": {
        element.type = getChildCharacters(eventReader, "type");
        break;
      }
      case "sat": {
        element.sat = Integer.parseInt(getChildCharacters(eventReader, "sat"));
        break;
      }
      case "dgpsid": {
        element.dgpsid = Integer.parseInt(getChildCharacters(eventReader, "dgpsid"));
        break;
      }
      case "vdop": {
        element.vdop = Double.parseDouble(getChildCharacters(eventReader, "vdop"));
        break;
      }
      case "fix": {
        element.fix = getChildCharacters(eventReader, "fix");
        break;
      }
      case "course": {
        element.course = Double.parseDouble(getChildCharacters(eventReader, "course"));
        break;
      }
      case "speed": {
        element.speed = Double.parseDouble(getChildCharacters(eventReader, "speed"));
        break;
      }
      case "hdop": {
        element.hdop = Double.parseDouble(getChildCharacters(eventReader, "hdop"));
        break;
      }
      case "pdop": {
        element.pdop = Double.parseDouble(getChildCharacters(eventReader, "pdop"));
        break;
      }
      case "url": {
        element.url = getChildCharacters(eventReader, "url");
        break;
      }
      case "number": {
        element.number = getChildCharacters(eventReader, "number");
        break;
      }
      case "urlname": {
        element.urlname = getChildCharacters(eventReader, "urlname");
        break;
      }
      case "time": {
        String timeStr = getChildCharacters(eventReader, "time");
        try {
          element.timestamp = GpxUtils.parseDateSeconds(timeStr).getTime();

        } catch (final ParseException e) {
          try {
            element.timestamp = GpxUtils.parseDateMillis(timeStr).getTime();
          } catch (final ParseException e2) {
            if (timeStr.length() > 20) {
              final String secTimeStr = timeStr.substring(timeStr.length() - 20);

              try {
                element.timestamp = GpxUtils.parseDateSeconds(secTimeStr).getTime();
              } catch (final ParseException e3) {
                if (timeStr.length() > 27) {
                  timeStr = timeStr.substring(timeStr.length() - 27);
                  try {
                    element.timestamp = GpxUtils.parseDateMillis(timeStr).getTime();
                  } catch (final ParseException e4) {
                    e4.printStackTrace();
                  }
                }
              }
            } else {
              e2.printStackTrace();
            }
          }
        }
        break;
      }
      default:
        return false;
    }
    return true;
  }

  static final NumberFormat LatLongFormat = new DecimalFormat("0000000000");

  private static String toID(final Double val) {
    return LatLongFormat.format(val.doubleValue() * 10000000);
  }

  private static void processElementAttributes(
      final StartElement node,
      final GPXDataElement element) throws Exception {
    @SuppressWarnings("unchecked")
    final Iterator<Attribute> attributes = node.getAttributes();
    while (attributes.hasNext()) {
      final Attribute a = attributes.next();
      if (a.getName().getLocalPart().equals("lon")) {
        element.lon = Double.parseDouble(a.getValue());
      } else if (a.getName().getLocalPart().equals("lat")) {
        element.lat = Double.parseDouble(a.getValue());
      }
    }
  }

  private static class GPXDataElement {

    Long timestamp = null;
    Integer dgpsid = null;
    Double elevation = null;
    Double lat = null;
    Double lon = null;
    Double course = null;
    Double speed = null;
    Double magvar = null;
    Double geoidheight = null;
    String name = null;
    String cmt = null;
    String desc = null;
    String src = null;
    String fix = null;
    String link = null;
    String sym = null;
    String type = null;
    String url = null;
    String urlname = null;
    Integer sat = null;
    Double hdop = null;
    Double pdop = null;
    Double vdop = null;
    String elementType;
    // over-rides id
    String number = null;

    Coordinate coordinate = null;
    List<GPXDataElement> children = null;
    GPXDataElement parent;
    long id = 0;
    int childIdCounter = 0;

    double maxLineLength = Double.MAX_VALUE;

    public GPXDataElement(final String myElType) {
      elementType = myElType;
    }

    public GPXDataElement(final String myElType, final double maxLength) {
      elementType = myElType;
      maxLineLength = maxLength;
    }

    @Override
    public String toString() {
      return elementType;
    }

    public String getPath() {
      final StringBuffer buf = new StringBuffer();
      GPXDataElement currentGP = parent;
      buf.append(elementType);
      while (currentGP != null) {
        buf.insert(0, '.');
        buf.insert(0, currentGP.elementType);
        currentGP = currentGP.parent;
      }
      return buf.toString();
    }

    public void addChild(final GPXDataElement child) {

      if (children == null) {
        children = new ArrayList<>();
      }
      children.add(child);
      child.parent = this;
      child.id = ++childIdCounter;
    }

    public String composeID(
        final String prefix,
        final boolean includeLatLong,
        final boolean includeParent) {
      // /top?
      if (parent == null) {
        if ((prefix != null) && (prefix.length() > 0)) {
          return prefix;
        }
      }

      final StringBuffer buf = new StringBuffer();
      if ((parent != null) && includeParent) {
        final String parentID = parent.composeID(prefix, false, true);
        if (parentID.length() > 0) {
          buf.append(parentID);
          buf.append('_');
        }
        if ((number != null) && (number.length() > 0)) {
          buf.append(number);
        } else {
          buf.append(id);
        }
        buf.append('_');
      }
      if ((name != null) && (name.length() > 0)) {
        buf.append(name.replaceAll("\\s+", "_"));
        buf.append('_');
      }
      if (includeLatLong && (lat != null) && (lon != null)) {
        buf.append(toID(lat)).append('_').append(toID(lon));
        buf.append('_');
      }
      if (buf.length() > 0) {
        buf.deleteCharAt(buf.length() - 1);
      }
      return buf.toString();
    }

    public Coordinate getCoordinate() {
      if (coordinate != null) {
        return coordinate;
      }
      if ((lat != null) && (lon != null)) {
        coordinate = new Coordinate(lon, lat);
      }
      return coordinate;
    }

    public boolean isCoordinate() {
      return (lat != null) && (lon != null);
    }

    public List<Coordinate> buildCoordinates() {
      if (isCoordinate()) {
        return Arrays.asList(getCoordinate());
      }
      final ArrayList<Coordinate> coords = new ArrayList<>();
      for (int i = 0; (children != null) && (i < children.size()); i++) {
        coords.addAll(children.get(i).buildCoordinates());
      }
      return coords;
    }

    private Long getStartTime() {
      if (children == null) {
        return timestamp;
      }
      long minTime = Long.MAX_VALUE;
      for (final GPXDataElement element : children) {
        final Long t = element.getStartTime();
        if (t != null) {
          minTime = Math.min(t.longValue(), minTime);
        }
      }
      return (minTime < Long.MAX_VALUE) ? Long.valueOf(minTime) : null;
    }

    private Long getEndTime() {
      if (children == null) {
        return timestamp;
      }
      long maxTime = 0;
      for (final GPXDataElement element : children) {
        final Long t = element.getEndTime();
        if (t != null) {
          maxTime = Math.max(t.longValue(), maxTime);
        }
      }
      return (maxTime > 0) ? Long.valueOf(maxTime) : null;
    }

    public boolean build(
        final SimpleFeatureBuilder builder,
        final Long backupTimestamp,
        final boolean timeRange) {
      if ((lon != null) && (lat != null)) {
        final Coordinate p = getCoordinate();
        builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
        builder.set("Latitude", lat);
        builder.set("Longitude", lon);
      }
      setAttribute(builder, "Elevation", elevation);
      setAttribute(builder, "Course", course);
      setAttribute(builder, "Speed", speed);
      setAttribute(builder, "Source", src);
      setAttribute(builder, "Link", link);
      setAttribute(builder, "URL", url);
      setAttribute(builder, "URLName", urlname);
      setAttribute(builder, "MagneticVariation", magvar);
      setAttribute(builder, "Satellites", sat);
      setAttribute(builder, "Symbol", sym);
      setAttribute(builder, "VDOP", vdop);
      setAttribute(builder, "HDOP", hdop);
      setAttribute(builder, "GeoHeight", geoidheight);
      setAttribute(builder, "Fix", fix);
      setAttribute(builder, "Station", dgpsid);
      setAttribute(builder, "PDOP", pdop);
      setAttribute(builder, "Classification", type);
      setAttribute(builder, "Name", name);
      setAttribute(builder, "Comment", cmt);
      setAttribute(builder, "Description", desc);
      setAttribute(builder, "Symbol", sym);
      if (timestamp != null) {
        setAttribute(builder, "Timestamp", new Date(timestamp));
      } else if ((backupTimestamp != null) && !timeRange) {
        setAttribute(builder, "Timestamp", new Date(backupTimestamp));
      }
      if (children != null) {

        boolean setDuration = true;

        final List<Coordinate> childSequence = buildCoordinates();

        final int childCoordCount = childSequence.size();
        if (childCoordCount <= 1) {
          return false;
        }

        final LineString geom =
            GeometryUtils.GEOMETRY_FACTORY.createLineString(
                childSequence.toArray(new Coordinate[childSequence.size()]));

        // Filter gpx track based on maxExtent
        if (geom.isEmpty() || (geom.getEnvelopeInternal().maxExtent() > maxLineLength)) {
          return false;
        }

        builder.set("geometry", geom);

        setAttribute(builder, "NumberPoints", Long.valueOf(childCoordCount));

        Long minTime = getStartTime();
        if (minTime != null) {
          builder.set("StartTimeStamp", new Date(minTime));
        } else if ((timestamp != null) && timeRange) {
          minTime = timestamp;
          builder.set("StartTimeStamp", new Date(timestamp));
        } else if ((backupTimestamp != null) && timeRange) {
          minTime = backupTimestamp;
          builder.set("StartTimeStamp", new Date(backupTimestamp));
        } else {
          setDuration = false;
        }
        Long maxTime = getEndTime();
        if (maxTime != null) {
          builder.set("EndTimeStamp", new Date(maxTime));
        } else if ((timestamp != null) && timeRange) {
          maxTime = timestamp;
          builder.set("EndTimeStamp", new Date(timestamp));
        } else if ((backupTimestamp != null) && timeRange) {
          maxTime = backupTimestamp;
          builder.set("EndTimeStamp", new Date(backupTimestamp));
        } else {
          setDuration = false;
        }
        if (setDuration) {
          builder.set("Duration", maxTime - minTime);
        }
      }
      return true;
    }
  }

  private static void setAttribute(
      final SimpleFeatureBuilder builder,
      final String name,
      final Object obj) {
    if ((builder.getFeatureType().getDescriptor(name) != null) && (obj != null)) {
      builder.set(name, obj);
    }
  }
}
