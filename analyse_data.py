# -*- coding: utf-8 -*-

import csv
import sys
import urllib
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, avg, mean, length
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType,FloatType

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

    # Initialisation et configuration de la session spark
    conf = SparkConf().setAppName("AirBnb - Formation 29")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    dataframe = spark.read.csv("/user/formation29/airbnb_clean.csv", header=True, sep='‽', inferSchema=True)
    dataframe.select("review_scores_rating").show(n=1000)

    dataframe = dataframe.withColumn("review_scores_rating", dataframe.review_scores_rating.cast(FloatType()))
    dataframe = dataframe.withColumn("price",dataframe.price.cast(DoubleType()))
    dataframe = dataframe.withColumn("room_type", dataframe.room_type.cast(IntegerType()))

    dataframe.select("review_scores_rating").show()
    # Recuperation des meilleurs logements (note superieure ou egale a 4)
    dataframeReferenceListing = dataframe.filter(dataframe.review_scores_rating >= 4)

    # Recuperation des logements qui ont besoin d'aide (note strictement inferieure a 4)
    dataframeListingToHelp = dataframe.filter(dataframe.review_scores_rating < 4)

    # Initialisation du tableau qu'on enregistrera en CSV a la fin du programme
    listingsToHelp = []

    # Initialisation de la table de jointure entre les logements a aider et les logements de reference
    j_ListingReference_ListingToHelp = []

    # Initialisation de la table de jointure entre la liste des logements a aider et les ameliorations a faire sur le logement
    listingsImprovements = []


    for listing in dataframeListingToHelp.collect():

        # Initialisation et construction de l'objet qu'on inserera dans listingsToHelp
        listingToHelp = {}
        listingToHelp['listing_id'] = listing.id
        listingToHelp['listing_name'] = listing.name
        listingToHelp['listing_url'] = listing.listing_url
        listingToHelp['host_name'] = listing.host_name
        listingsToHelp.append(listingToHelp)
        # On recupere les logements similaires dans la liste des logements de reference
        dataframeSimilarListing = dataframeReferenceListing.filter(
          (dataframeReferenceListing.price < 1.1*listing.price) &
          (dataframeReferenceListing.price > 0.9*listing.price) &
          (dataframeReferenceListing.neighbourhood_cleansed == listing.neighbourhood_cleansed) &
          (dataframeReferenceListing.accommodates == listing.accommodates) &
          (dataframeReferenceListing.room_type == listing.room_type) &
          (dataframeReferenceListing.bedrooms == listing.bedrooms)
        )

        if dataframeSimilarListing.count() == 0:
          continue

        if listing.host_has_profile_pic is not True:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 6})

        if listing.host_identity_verified is not True:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 7})

        avgDescriptionLength = dataframeSimilarListing.agg(mean(length(col("description"))))
        if listing.description is not None and len(listing.description) < avgDescriptionLength:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 5})

        avgHostAcceptanceRate = dataframeSimilarListing.select(avg("host_acceptance_rate")).collect()[0][0]
        if listing.host_acceptance_rate < avgHostAcceptanceRate:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 4})

        avgHostResponseTime = dataframeSimilarListing.select(avg("host_response_time")).collect()[0][0]
        if listing.host_response_time < avgHostResponseTime:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 3})

        avgAmenities = dataframeSimilarListing.select(avg("amenities")).collect()[0][0]
        if len(listing.amenities) < avgAmenities:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 3})

        for reference in dataframeSimilarListing.collect():
            j_ListingReference_ListingToHelp.append({"listing_id": listing.id, "reference_id": reference.id})
            
with open('jListingReference_ListingToHelp.csv', 'wb') as j_Help_Reference:
    keys = j_ListingReference_ListingToHelp[0].keys()
    csvDictWriter = csv.DictWriter(j_Help_Reference, keys)
    csvDictWriter.writeheader()
    csvDictWriter.writerows(j_ListingReference_ListingToHelp)

with open('listingsImprovements.csv', 'wb') as listingsImprovementsFile:
    keys = listingsImprovements[0].keys()
    csvDictWriter = csv.DictWriter(listingsImprovementsFile, keys)
    csvDictWriter.writeheader()
    csvDictWriter.writerows(listingsImprovements)

with open('listingsToHelp.csv', 'wb') as listingsToHelpFile:
    keys = listingsToHelp[0].keys()
    csvDictWriter = csv.DictWriter(listingsToHelpFile, keys)
    csvDictWriter.writeheader()
    csvDictWriter.writerows(listingsToHelp)
