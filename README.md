# GPX Planet to PostgreSQL

This is a simple tool that stores GPX dump in a PostgreSQL database with PostGIS extension.

## Usage

Run `gpx2pgsql.py -h` to see the list of options. The following command imports
a test GPX dump, creating (or recreating) tables, asking for password and reprojecting
coordinates to EPSG:900913 for usage with an example mapnik style:

    xzcat gpx-planet-test-003.tar.xz | gpx2pgsql.py -w -c -9 -f -

This script will skip tracks with IDs already in the database, so if it's stopped, the processing could be resumed with the same command (bar `-c` switch, of course).

The 391 MB xzipped test data needed 1098 MB in a database, so one can expect only tripling in size, not hundreds of gigabytes.

## Example Mapnik Style

See `mapnik-gpx.xml` for an example of a rendering style for imported GPS traces.

## License

This script was written by Ilya Zverev and licensed WTFPL.

