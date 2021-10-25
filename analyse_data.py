import sys
import urllib
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, avg
from pyspark.sql.types import IntegerType,BooleanType,DateType

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

    # Initialisation et configuration de la session spark
    conf = SparkConf().setAppName("AirBnb - Formation 29")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    dataframe = spark.read.csv("/user/formation29/airbnb_clean.csv", header=True, sep='$')

    dataframe.withColumn("price",dataframe.price.cast(BooleanType()))
    dataframe.withColumn("room_type", dataframe.room_type.cast(IntegerType()))
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

        # Initialisation et construction de l'objet qu'on inserera dans listingsToHelp
        listingToHelp = {}
        listingToHelp['listing_id'] = listing.id
        listingToHelp['listing_name'] = listing.name
        listingToHelp['listing_url'] = listing.listing_url
        listingToHelp['host_name'] = listing.host_name
        listingToHelp['reference_listing_url'] = []

        # On recupere les logements similaires dans la liste des logements de reference
        dataframeSimilarListing = dataframeReferenceListing.filter(
        dataframeReferenceListing.room_type == listing.room_type
        )

        print(dataframeSimilarListing.room_type)
        print("========")
        print(listing.room_type)

        if listing.host_has_profile_pic is not True:
            listingsImprovements.append({"listing_id": listing.id, "improvement_id": 6})

        if listing.host_identity_verified is not True:
        listingsImprovements.append({"listing_id": listing.id, "improvement_id": 7})

        avgDescriptionLength = dataframeSimilarListing.select(avg("description")).collect()[0][0]
        if len(listing.description) < avgDescriptionLength:
        listingsImprovements.append({"listing_id": listing.id, "improvement_id": 5})

        avgHostAcceptanceRate = dataframeSimilarListing.select(avg("host_acceptance_rate")).collect()[0][0]
        if listing.host_acceptance_rate < avgHostAcceptanceRate:
        listingsImprovements.append({"listing_id": listing.id, "improvement_id": 4})

        avgHostResponseTime = dataframeSimilarListing.select(avg("host_response_time")).collect()[0][0]
        if listing.host_response_time < avgHostResponseTime:
        listingsImprovements.append({"listing_id": listing.id, "improvement_id": 3})

        for reference in dataframeSimilarListing.collect():
        j_ListingReference_ListingToHelp.append({"listing_id": listing.id, "reference_id": reference.id})


  print(j_ListingReference_ListingToHelp)
