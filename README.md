# GPX Planet to PostgreSQL

This is a simple tool that stores GPX dump in a PostgreSQL databae with PostGIS extension.

## Usage

Run `gpx2pgsql.py -h` to see the list of options. The following command imports
a test GPX dump, creating (or recreating) tables, asking for password and reprojecting
coordinates to EPSG:900913 for usage with an example mapnik style:

    xzcat gpx-planet-test-003.tar.xz | gpx2pgsql.py -w -c -9 -f -

## Example Mapnik Style

See `mapnik-gpx.xml` for an example of a rendering style for imported GPS traces.

## License

This script was written by Ilya Zverev and licensed WTFPL.

