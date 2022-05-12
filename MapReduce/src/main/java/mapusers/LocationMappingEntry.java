package mapusers;

public class LocationMappingEntry {
    private final String weirdLocation;
    private final String country;

    public LocationMappingEntry(String weirdLocation, String country) {
        this.weirdLocation = weirdLocation;
        this.country = country;
    }

    public String getWeirdLocation() {
        return weirdLocation;
    }

    public String getCountry() {
        return country;
    }
}
