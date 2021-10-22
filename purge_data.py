import pandas as pd
from pandas.io.json import json_normalize
import pandas as pd
import json
from ast import literal_eval
import datetime
import numpy as np
 
listingCsv = pd.read_csv('./listings.csv', usecols=['id', 'listing_url', 'name', 'description', 'neighborhood_overview', 'picture_url',
                                                	'host_name', 'host_since', 'host_about',  'host_response_time', 'host_response_rate', 'host_acceptance_rate',
                                                	'host_is_superhost', 'host_picture_url', 'host_verifications', 'host_has_profile_pic', 'host_identity_verified',
                                                	'neighbourhood', 'neighbourhood_cleansed', 'latitude', 'longitude', 'room_type',
                                                	'accommodates', 'bathrooms_text', 'bedrooms', 'beds', 'amenities', 'price', 'minimum_nights', 'maximum_nights', 'has_availability',
                                                	'availability_30', 'availability_60', 'availability_90', 'availability_365', 'number_of_reviews', 'number_of_reviews_ltm',
                                                	'number_of_reviews_l30d', 'last_review', 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness',
                                                	'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value', 'instant_bookable',
                                                	'reviews_per_month'],
                    	parse_dates=['last_review', 'host_since'])
dataframe = pd.DataFrame(listingCsv)
 
 
# On commence par une phase de traitement de nos données, pour pouvoir manipuler nos données plus facilement lors de la phase d'analyse
 
# On supprime les logements qui n'ont pas eu d'avis depuis plus de 3 ans. On les considère comme "inactif".
dataframe = dataframe[dataframe['last_review'] > (datetime.datetime.now() - datetime.timedelta(days=3*365))]
 
# Amenities et host_verifications contiennent une chaîne de caractères représentant un tableau, on transforme les données de ces champs en véritable tableau python
dataframe['amenities'] = dataframe['amenities'].apply(literal_eval)
dataframe['host_verifications'] = dataframe['host_verifications'].apply(literal_eval)
 
 
# On transforme les champs string qui contiennent 't' et 'f' (respectivement pour True et False) en véritables booléens
boolString = {'t': True, 'f': False, '': False, np.nan: False}
dataframe['has_availability'] = dataframe['has_availability'].map(boolString)
dataframe['host_has_profile_pic'] = dataframe['host_has_profile_pic'].map(boolString)
dataframe['host_identity_verified'] = dataframe['host_identity_verified'].map(boolString).fillna(False)
dataframe['host_is_superhost'] = dataframe['host_is_superhost'].map(boolString)
dataframe['instant_bookable'] = dataframe['instant_bookable'].map(boolString)
 
 
 
# On transforme les chaînes de caractères représentant le temps de réponse en utilisant un dictionnaire
responseTime = {'N/A': 5, 'within an hour': 1, 'within a few hours': 2, 'within a day': 3, 'a few days or more': 4}
dataframe['host_response_time'] = dataframe['host_response_time'].map(responseTime)
dataframe['host_response_time'] = dataframe['host_response_time'].fillna(5)
 
# On transforme les pourcentages en float
dataframe['host_response_rate'] = dataframe['host_response_rate'].str.rstrip('%').astype('float') / 100.0
dataframe['host_acceptance_rate'] = dataframe['host_acceptance_rate'].str.rstrip('%').astype('float') / 100.0
 
# On supprime la monnaie dans le prix du logement et on convertit la chaîne en float
dataframe['price'] = dataframe['price'].str[1:]
dataframe['price'] = dataframe['price'].str.replace(',', '')
dataframe['price'] = dataframe['price'].astype(float).fillna(0.0)
 
# On convertit les scores de reviews ou certaines colonnes sont sur 100, d'autres sur 10, d'autre sur 5 pour un score de ref sur 5
dataframe['review_scores_rating'] = dataframe['review_scores_rating'] / 20
dataframe['review_scores_accuracy'] = dataframe['review_scores_rating'] / 2
dataframe['review_scores_cleanliness'] = dataframe['review_scores_rating'] / 2
dataframe['review_scores_checkin'] = dataframe['review_scores_rating'] / 2
dataframe['review_scores_communication'] = dataframe['review_scores_rating'] / 2
dataframe['review_scores_location'] = dataframe['review_scores_rating'] / 2
dataframe['review_scores_value'] = dataframe['review_scores_rating'] / 2
 
# On récupère juste la parti du nombre de bath, pas le text
dataframe['bathrooms_text'] = dataframe['bathrooms_text'].str[0:1]

# Pareil avec l'arondissement.
dataframe['neighbourhood_cleansed'] = dataframe['neighbourhood_cleansed'].str[0:1]
 
 
# On transforme les chaînes de caractères représentant le type de room en utilisant un dictionnaire
responseRoom = {'Private room': 1,
                	'Private room in apartment': 2,
                	'Private room in condominium': 3,
                	'Hotel room': 4,
                	'Room in boutique Hotel': 5,
                	'Shared room': 6,
                	'Entire apartment': 7,
                	'Entire condominium': 8,
                	'Entire loft': 9,
                	'Entire home/apt': 10}
dataframe['room_type'] = dataframe['room_type'].map(responseRoom)
 
# On remplace les lignes qui ne sont pas du bon type
# Integer
dataframe['host_response_rate'].replace(np.nan, 0, inplace=True)
dataframe['host_acceptance_rate'].replace(np.nan, 0, inplace=True)
dataframe['bedrooms'].replace(np.nan, 0, inplace=True)

# String
dataframe['description'].replace(np.nan, '', inplace=True)
dataframe['neighborhood_overview'].replace(np.nan, '', inplace=True)
dataframe['host_about'].replace(np.nan, '', inplace=True)

dataframe['room_type'].replace('', np.nan, inplace=True)
dataframe.dropna(subset=['room_type'], inplace=True)

dataframe.to_csv('./airbnb_clean.csv')
