package iu.pti.hbaseapp.truthy;

import java.lang.Math;
/*
 * Helper class for working with geo-coordinates.
 * Geo-coordinates are defined as strings of the form: "LONGITUDE_LATITUDE"
 * e.g. "-9726_3548"
 */
public class GeoCoordinatesHelper {

	public static final String LON_LAT_DELIM = "_";
	
    private String[] geoLocForBox = new String[2]; // original geo-locations
    private Integer[] lonlat1 = new Integer[2]; // Longitude-Latitude of these geolocations
    private Integer[] lonlat2 = new Integer[2]; 

    private static final Integer[] LON = { -18000, 18000 }; // e.g. -180.00 * precision factor
    private static final Integer[] LAT = { -9000, 9000 };
    private Integer[] nextCoordinates = new Integer[2];
    private Integer minLon, maxLon, minLat, maxLat;

    /*
     * Setter method for defining the rectangular box
     * whose cells are to be enumerated.
     */
    public void setCoordinatesBoxUsing(String geoLoc1, String geoLoc2) 
        throws Exception {
        geoLocForBox[0] = geoLoc1;
        geoLocForBox[1] = geoLoc2;
        lonlat1 = getLonLatFromCoordinates(geoLoc1);
        lonlat2 = getLonLatFromCoordinates(geoLoc2);
        minLon = Math.min(lonlat1[0], lonlat2[0]);
        maxLon = Math.max(lonlat1[0], lonlat2[0]);
        minLat = Math.min(lonlat1[1], lonlat2[1]);
        maxLat = Math.max(lonlat1[1], lonlat2[1]);
        if (!(minLon >= this.LON[0] 
                && maxLon <= this.LON[1]) 
            || !(minLat >= this.LAT[0] 
                && maxLat <= this.LAT[1])) {
            throw new Exception("Invalid geo-coordinates.");
        }
        nextCoordinates[0] = minLon;
        nextCoordinates[1] = minLat;
    }

    /*
     * Check whether there is additional point in the box.
     */
    public boolean hasNext(){
        if (nextCoordinates == null)
            return false;
        return true;
    }

    /*
     * This method updates the internal state of the bounding box - the
     * next state (lon_lat) to return.
     */
    private void updateNextCoordinates(){
        if (nextCoordinates[0] + 1 <= maxLon) {
            nextCoordinates[0] += 1;
        } else if (nextCoordinates[1] + 1 <= maxLat) {
            nextCoordinates[1] += 1;
            nextCoordinates[0] = minLon;
        } else {
            nextCoordinates = null;
        }
    }

    /*
     * Method that returns the next coordinates in the rectangular box.
     * New geo-coordinates are generated left-to-right, bottom-to-top fashion.
     * e.g. All longitudes of the box are enumerated first, followed by latitudes.
     */
    public String getNextGeoCoordinatesStr() 
        throws Exception {
        if (!hasNext()) {
            throw new Exception(" Out of coordinates' bounding box.");
        }
        Integer[] temp = nextCoordinates.clone();
        updateNextCoordinates();
        return getCooFromLonLat(temp);
    }

	/*
	 * Generic method to get a string representation of longitude, latitude.
	 * e.g. [-9725, 3548] is returned as "-9725_3548".
	 */
	private String getCooFromLonLat(Integer[] lonlat) {
		return String.valueOf(lonlat[0]) 
				+ GeoCoordinatesHelper.LON_LAT_DELIM 
				+ String.valueOf(lonlat[1]);
	}

	/*
	 * Generic method to convert any string representation of geo-coordinates
	 * to integer-based geo-coodinates.
	 * e.g. "-9725_3548" are returned as [-9725, 3548].
	 * 
	 * Complements static method getCooFromLonLat().
	 */
	private Integer[] getLonLatFromCoordinates(String s) {
		String[] lonlatstr = s.split(GeoCoordinatesHelper.LON_LAT_DELIM);
		Integer[] lonlat = new Integer[2];
		lonlat[0] = Integer.parseInt(lonlatstr[0]);
		lonlat[1] = Integer.parseInt(lonlatstr[1]);
		return lonlat;
	}

	public static void main(String []args){
		try {
		    GeoCoordinatesHelper geo = new GeoCoordinatesHelper();
			geo.setCoordinatesBoxUsing("2226_3048", "2365_3280"); // random (small)
//			geo.setCoordinatesBoxUsing("-9726_3548", "2365_580"); // random (medium)
//			geo.setCoordinatesBoxUsing("-1800_-9000", "18000_9000"); // whole world (huge)
//			geo.setCoordinatesBoxUsing("-8754_3796", "-8481_4176"); // roughly, Indiana state boundary
			// roughly, USA: lower left near San Diego, 
			// right top north of Vermont, in line with northern border of North Dakota
//			geo.setCoordinatesBoxUsing("-11720_3255", "-7348_4910"); // ~7.2million coords  
			int count = 0;
			while(geo.hasNext()) {
				count += 1;
				System.out.println(count + ": " + geo.getNextGeoCoordinatesStr());
			}
			System.out.println("\nTotal: " + count);
		} catch(Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
		System.out.println("\n\n");
	}
}
