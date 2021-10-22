from pyspark.sql.types import StructType

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

  # Initialisation et configuration de la session spark
  conf = SparkConf().setAppName("AirBnb - Formation 29")
  sc = SparkContext(conf=conf)
  spark = SparkSession(sc)

  dataframe = spark.read.csv("/user/formation29/airbnb_clean.csv", header=True)

  # Recuperation des meilleurs logements (note superieure ou egale a 4)
  dataframeReferenceListing = dataframe.filter(dataframe.review_scores_rating >= 4)

  # Recuperation des logements qui ont besoin d'aide (note strictement inferieure a 4)
  dataframeListingToHelp = dataframe.filter(dataframe.review_scores_rating < 4)

  # Initialisation du tableau qu'on enregistrera en CSV a la fin du programme
  listingsToHelp = []

  listingsImprovements = []
  # On boucle sur la liste des annonces a aider
  for listing in dataframeListingToHelp:

    # Initialisation et construction de l'objet qu'on inserera dans listingsToHelp
    listingToHelp = {}
    listingToHelp['listing_id'] = listing.id
    listingToHelp['listing_name'] = listing.name
    listingToHelp['listing_url'] = listing.listing_url
    listingToHelp['host_id'] = listing.host_id
    listingToHelp['host_name'] = listing.host_name
    listingToHelp['reference_listing_url'] = []


    # On recupere les logements similaires dans la liste des logements de reference
    dataframeSimilarListing = dataframeReferenceListing.filter(
        (0.9*dataframeReferenceListing.price < listing.price < 1.1*dataframeReferenceListing.price) &
        (dataframeReferenceListing.neighbourhood_cleansed == listing.neighbourhood_cleansed) &
        (dataframeReferenceListing.neighbourhood == listing.neighbourhood) &
        (dataframeBestHost.accommodates == listing.accommodates) &
        (dataframeBestHost.room_type == listing.room_type) &
        (dataframeBestHost.bedrooms == listing.bedrooms)
    )

    if listing.host_has_profile_pic is not True:
      listingsImprovements.append({"listing_id": listing.id, "improvement_id": 5})

    if listing.host_identity_verified is not True:
      listingsImprovements.append({"listing_id": listing.id, "improvement_id": 6})

    break