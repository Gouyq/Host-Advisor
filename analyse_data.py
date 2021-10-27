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

    class ImprovementListing(object):
        def __init__(self, listing_id, improvement_id):
                self.listing_id = listing_id
                self.improvement_id = improvement_id

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

        # Comparaison sur le nombre de services
        avgAmenities = dataframeSimilarListing.select(avg("amenities")).collect()[0][0]
        if len(listing.amenities) < avgAmenities:
            listingsImprovements.append(ImprovementListing(listing.id, 1).__dict__)

        # Comparaison sur le temps de reponse moyen de l'hote
        avgHostResponseTime = dataframeSimilarListing.select(avg("host_response_time")).collect()[0][0]
        if listing.host_response_time < avgHostResponseTime:
            listingsImprovements.append(ImprovementListing(listing.id, 2).__dict__)

        # Comparaison sur le taux d'acceptation des reservations
        avgHostAcceptanceRate = dataframeSimilarListing.select(avg("host_acceptance_rate")).collect()[0][0]
        if listing.host_acceptance_rate < avgHostAcceptanceRate:
            listingsImprovements.append(ImprovementListing(listing.id, 3).__dict__)

        # Comparaison sur le fait que l'hote ait une photo de profil
        if listing.host_has_profile_pic is not True:
            listingsImprovements.append(ImprovementListing(listing.id, 4).__dict__)

        # Comparaison sur le fait que l'hote soit verifie ou non
        if listing.host_identity_verified is not True:
            listingsImprovements.append(ImprovementListing(listing.id, 5).__dict__)

        # Comparaison sur la taille de la description
        avgDescriptionLength = dataframeSimilarListing.agg(mean(length(col("description"))))
        if listing.description is not None and len(listing.description) < avgDescriptionLength:
            listingsImprovements.append(ImprovementListing(listing.id, 6).__dict__)

        # On remplit la table de jointure entre les logements a aider et les logements de reference
        for reference in dataframeSimilarListing.collect():
            j_ListingReference_ListingToHelp.append({"listing_id": listing.id, "reference_id": reference.id})

    # Creation des fichiers qui seront utilises par la visualisation
    df_j_ListingReference_ListingToHelp = spark.createDataFrame(j_ListingReference_ListingToHelp)
    df_j_ListingReference_ListingToHelp.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').option('header','true').save('/user/formation29/jListingReference_ListingToHelp.csv')

    df_listingsToHelp = spark.createDataFrame(listingsToHelp)
    df_listingsToHelp.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').option('header','true').save('/user/formation29/listingsToHelp.csv')

    df_listingsImprovements = spark.createDataFrame(listingsImprovements)
    df_listingsImprovements.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').option('header','true').save('/user/formation29/listingsImprovements.csv')
