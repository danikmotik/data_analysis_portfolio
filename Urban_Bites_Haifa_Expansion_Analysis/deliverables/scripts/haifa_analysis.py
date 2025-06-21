#
# This file is used to upload raw data, process it and save it as "node_dataset.csv".
# The file presents the crucial data about nodes on the map.
# The exported file will further on be used in "map_data.py" for merging the datasets.
# Along with data manipulation calculations are done.
#


import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.stats import pearsonr


# Load the businesses data
businesses = gpd.read_file("data/all_businesses.geojson")
# Load neighborhood data
neighborhoods = gpd.read_file("data/Haifa GeoJSON.geojson")


types = ['amenity', 'shop', 'office', 'leisure', 'tourism']
col = ['name', 'geometry']


def get_category(row):
    for t in types:
        if pd.notna(row[t]):
            return t
    return None


def get_subcategory(row):
    return row[row['category']]


def filtering_businesses():
    filtered_businesses = businesses[businesses[types].notna().any(axis=1)]
    filtered_businesses = filtered_businesses[col + types]
    filtered_businesses['category'] = filtered_businesses.apply(get_category, axis=1)
    filtered_businesses['subcategory'] = filtered_businesses.apply(get_subcategory, axis=1)
    filtered_businesses = filtered_businesses.drop(columns=types)
    return filtered_businesses


def spatial_join_for_geometry(filtered_businesses):
    joined = gpd.sjoin(filtered_businesses, neighborhoods, how="left", predicate="within")
    return joined


def prepare_table(joined):
    to_export = joined[['name', 'category', 'subcategory', 'SchName']]
    return to_export


def export_file(to_export, name):
    to_export.to_csv(name, index=False)


### EXECUTE ###

fb = filtering_businesses()
joined_table = spatial_join_for_geometry(fb)
table = prepare_table(joined_table)

# # Check what businesses are there
# print(table['subcategory'].unique())
# --------------->
business_subcategories = [
    'restaurant', 'fast_food', 'cafe', 'kiosk', 'hotel', 'hostel', 'guest_house',
    'supermarket', 'bakery', 'convenience', 'variety_store', 'stationery',
    'clothes', 'shoes', 'books', 'gift', 'jewelry', 'florist', 'tattoo', 'bar',
    'pub', 'nightclub', 'hairdresser', 'beauty', 'dentist', 'clinic',
    'pharmacy', 'veterinary', 'optician', 'cosmetics', 'lawyer', 'travel_agency',
    'bureau_de_change', 'photo', 'confectionery', 'toys', 'electronics', 'mobile_phone',
    'computer', 'department_store', 'lottery', 'laundry', 'furniture', 'ice_cream',
    'houseware', 'greengrocer', 'deli', 'hardware', 'butcher', 'copyshop',
    'craft', 'curtain', 'baby_goods', 'camera', 'fitness_centre', 'company', 'bed',
    'home', 'tea', 'coffee', 'e-cigarette', 'apartment', 'car_rental', 'car_parts',
    'car_repair', 'bicycle', 'travel', 'sports', 'plastics'
]

non_business_subcategories = [
    'parking', 'fuel', 'information', 'viewpoint', 'drinking_water', 'attraction',
    'playground', 'post_office', 'bank', 'place_of_worship', 'sports_centre',
    'telephone', 'fire_station', 'school', 'kindergarten', 'theatre', 'artwork',
    'shelter', 'hospital', 'pitch', 'community_centre', 'post_box', 'bench',
    'recycling', 'vending_machine', 'swimming_pool', 'picnic_table', 'slipway',
    'food_court', 'taxi', 'dojo', 'billiards', 'museum', 'library', 'fountain',
    'park', 'picnic_site', 'waste_basket', 'atm', 'government', 'bus_station',
    'tyres', 'marina', 'yes', 'dog_park', 'garden', 'waste_disposal', 'lighting',
    'nuts', 'studio', 'diplomatic', 'ferry_terminal', 'bicycle_parking',
    'bookmaker', 'fitness_station', 'college', 'motorcycle_parking',
    'charging_station', 'trampoline_park', 'outpost', 'radiotechnics',
    'townhall', 'tobacco', 'bicycle_repair_station'
]

# # Check what food related businesses are there
# print(table.loc[table['category'] == 'amenity','subcategory'].unique())
# --------------->
food_subcats = [
    'restaurant',
    'fast_food',
    'cafe',
    'food_court',
    'ice_cream',
    'pub',
    'bar',
    'vending_machine',
    'bakery',
    'deli',
    'tea',
    'coffee'
]

# Get table with businesses only
businesses_table = table.loc[table['subcategory'].isin(business_subcategories)]

# Get total businesses per neighborhood
total_bus_neighborhood = businesses_table.groupby('SchName').size()
total_bus_neighborhood = total_bus_neighborhood.reset_index(name='count')

# Get table only for food related businesses
food_table = businesses_table.loc[businesses_table['subcategory'].isin(food_subcats)]

# AGG: Get sum of distinct subcategories per neighborhood
pre_diversity = businesses_table[['name', 'subcategory', 'SchName']].\
    groupby(['SchName', 'subcategory']).size()
pre_diversity = pre_diversity.reset_index(name='count')

diversity = pre_diversity.groupby(['SchName']).size()
diversity = diversity.reset_index(name='count')

# AGG: Get number of food related businesses by neighborhood and subcategory
food_by_neighborhood_subcat = food_table.groupby(['SchName', 'subcategory']).size()
food_by_neighborhood_subcat = food_by_neighborhood_subcat.reset_index(name='count')
food_by_neighborhood_subcat = food_by_neighborhood_subcat.sort_values(by=['SchName', 'count'])
food_by_neighborhood_subcat = food_by_neighborhood_subcat.reset_index(drop=True)

# AGG: Get number of food subcategories per neighborhood
food_subcat_num = food_by_neighborhood_subcat.groupby('SchName').size().sort_values()
food_subcat_num = food_subcat_num.reset_index(name='count')

# AGG: Get number of food businesses per neighborhood
food_by_neighborhood = food_table.groupby('SchName').size()
food_by_neighborhood = food_by_neighborhood.sort_values()
food_by_neighborhood = food_by_neighborhood.reset_index(name='count')

# JOIN: Compare total business num with food business num
total_and_food = pd.merge(food_by_neighborhood, total_bus_neighborhood, on='SchName', how='inner')
total_and_food = total_and_food.rename(columns={'count_x': 'total', 'count_y': 'food related'})
total_and_food['share_%'] = (total_and_food['total'] / total_and_food['food related'] * 100).round(1)

# JOIN: Compare share of food related businesses with the diversity in neighborhood
share_diversity = pd.merge(diversity, total_and_food, on='SchName', how='left')
share_diversity = share_diversity[['SchName', 'share_%', 'count']]
share_diversity = share_diversity.rename(columns={'count': 'diversity'})


# CALC: Get Pearson correlation and P values
def get_pearson(df):
    a = df.iloc[:, 0]
    b = df.iloc[:, 1]
    r, p = pearsonr(a, b)
    return r, p


def get_exponential_correlation(df, epsilon=1e-6):
    a = df.iloc[:, 0]  # Share of food businesses
    b = df.iloc[:, 1]  # Total businesses

    log_a = np.log(a + epsilon)
    r, p = pearsonr(log_a, b)
    return r, p


# OUTPUT: Get a comprehensive table for presentation
# 'SchName', 'latitude', 'longitude', 'total_businesses', 'total_food_businesses', 'diversity' 'share_%'
# share_diversity, food_by_neighborhood, total_bus_neighborhood
re_food_by_neighborhood = food_by_neighborhood.rename(columns={'count': 'total_food_businesses'})
re_total_bus_neighborhood = total_bus_neighborhood.rename(columns={'count': 'total_businesses'})

join_1 = pd.merge(share_diversity, re_total_bus_neighborhood, on='SchName', how='left')
join_2 = pd.merge(join_1, re_food_by_neighborhood, on='SchName', how='left')

join_2.to_csv('neighborhood_data', index=False)

# OUTPUT: Get complete dataset as CSV
# ['SchName', 'latitude', 'longitude', 'name', 'subcategory', 'is_business']
dataset = joined_table[['SchName', 'name', 'subcategory']].copy()
dataset['latitude'] = joined_table.geometry.x
dataset['longitude'] = joined_table.geometry.y
dataset['is_business'] = dataset['subcategory'].isin(business_subcategories)
dataset['is_food'] = dataset['subcategory'].isin(food_subcats)
# export_file(dataset, "node_dataset.csv")
# print(dataset.loc[dataset['is_food'] == True])


# CALC: Get total of food related businesses
food_total = food_by_neighborhood_subcat['count'].sum()
# print(food_total)
# = 293

# CALC: Get total neighboroods
total_neighborhood = table['SchName'].unique().size

# CALC: Sum distinct business categories
unique_subcat = businesses_table['subcategory'].unique().size
# print(unique_subcat)
# = 69

# checklist = []
# for i in business_subcategories:
#     if not (i in businesses_table['subcategory'].unique()):
#         checklist.append(i)

# pear = get_pearson(share_diversity.fillna(0)[['share_%', 'diversity']])
# print("Pearson: " + str(pear[0]))
# print("P-value: " + str(pear[1]))

exp_pear = get_exponential_correlation(share_diversity.fillna(0)[['share_%', 'diversity']])
print("Pearson: " + str(exp_pear[0]))
print("P-value: " + str(exp_pear[1]))
# print(share_diversity.fillna(0)[['share_%', 'diversity']])





