#
# This file is meant to extract the centroids of each neighborhood to present on the map
#


import geopandas as gpd


# Load file
gdf = gpd.read_file("data/Haifa GeoJSON.geojson")

# Change CRS
gdf_projected = gdf.to_crs(epsg=3857)

# Calculate centroids
gdf_projected['centroids'] = gdf_projected.geometry.centroid

# Change CRS back
centroid_gdf = gdf_projected.set_geometry('centroids').to_crs(epsg=4326)

# Get longitude and latitude
centroid_gdf['longitude'] = centroid_gdf.geometry.x
centroid_gdf['latitude'] = centroid_gdf.geometry.y

# Export to csv
centroid_gdf[['SchName', 'longitude', 'latitude']].to_csv('neighborhood_geopoints', index=False)
