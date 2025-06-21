#
# This file is used to create and export the complete dataset as CSV.
# The dataset includes ['SchName', 'name', 'subcategory', 'latitude_node', 'longitude_node',
# 'is_business', 'is_food', 'longitude_neighborhood', 'latitude_neighborhood']
#

import pandas as pd


# Load data
map_data = pd.read_csv('data/neighborhood_geopoints')
node_data = pd.read_csv('data/node_dataset.csv')

# Separate different coordinates
map_data = map_data.rename(columns={'latitude': 'latitude_neighborhood', 'longitude': 'longitude_neighborhood'})
node_data = node_data.rename(columns={'latitude': 'latitude_node', 'longitude': 'longitude_node'})

# Join tables
df = pd.merge(node_data, map_data, on='SchName', how='right')

# Save file to CSV
df.to_csv('Haifa_dataset_to_BI.txt', index=False)

# print(df.columns)