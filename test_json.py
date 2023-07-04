from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
spark = SparkSession.builder.appName('json_test').getOrCreate()

df = spark.read.option('multiline', 'True').json(r'dataset\file5.json')

print(df.printSchema())

df = df.withColumn('restaurants_explode', explode("restaurants"))\
    .withColumn('restaurants_restaurant', col('restaurants_explode.restaurant'))\
    .withColumn('restaurants_restaurant_R', col('restaurants_restaurant.R'))\
    .withColumn('restaurants_restaurant_R_res_id', col('restaurants_restaurant_R.res_id'))\
    .withColumn('restaurants_restaurant_apikey', col('restaurants_restaurant.apikey'))\
    .withColumn('restaurants_restaurant_average_cost_for_two', col('restaurants_restaurant.average_cost_for_two'))\
    .withColumn('restaurants_restaurant_cuisines', col('restaurants_restaurant.cuisines'))\
    .withColumn('restaurants_restaurant_currency', col('restaurants_restaurant.currency'))\
    .withColumn('restaurants_restaurant_deeplink', col('restaurants_restaurant.deeplink'))\
    .withColumn('restaurants_restaurant_establishment_types', col('restaurants_restaurant.establishment_types'))\
    .withColumn('restaurants_restaurant_events_url', col('restaurants_restaurant.events_url'))\
    .withColumn('restaurants_restaurant_featured_image', col('restaurants_restaurant.featured_image'))\
    .withColumn('restaurants_restaurant_has_online_delivery', col('restaurants_restaurant.has_online_delivery'))\
    .withColumn('restaurants_restaurant_has_table_booking', col('restaurants_restaurant.has_table_booking'))\
    .withColumn('restaurants_restaurant_id', col('restaurants_restaurant.id'))\
    .withColumn('restaurants_restaurant_is_delivering_now', col('restaurants_restaurant.is_delivering_now'))\
    .withColumn('restaurants_restaurant_location', col('restaurants_restaurant.location'))\
    .withColumn('restaurants_restaurant_location_address', col('restaurants_restaurant_location.address'))\
    .withColumn('restaurants_restaurant_location_city', col('restaurants_restaurant_location.city'))\
    .withColumn('restaurants_restaurant_location_city_id', col('restaurants_restaurant_location.city_id'))\
    .withColumn('restaurants_restaurant_location_country_id', col('restaurants_restaurant_location.country_id'))\
    .withColumn('restaurants_restaurant_location_latitude', col('restaurants_restaurant_location.latitude'))\
    .withColumn('restaurants_restaurant_location_locality', col('restaurants_restaurant_location.locality'))\
    .withColumn('restaurants_restaurant_location_locality_verbose', col('restaurants_restaurant_location.locality_verbose'))\
    .withColumn('restaurants_restaurant_location_longitude', col('restaurants_restaurant_location.longitude'))\
    .withColumn('restaurants_restaurant_menu_url', col('restaurants_restaurant.menu_url'))\
    .withColumn('restaurants_restaurant_name', col('restaurants_restaurant.name'))\
    .withColumn('restaurants_restaurant_offers', col('restaurants_restaurant.offers'))\
    .withColumn('restaurants_restaurant_photos_url', col('restaurants_restaurant.photos_url'))\
    .withColumn('restaurants_restaurant_price_range', col('restaurants_restaurant.price_range'))\
    .withColumn('restaurants_restaurant_switch_to_order_menu', col('restaurants_restaurant.switch_to_order_menu'))\
    .withColumn('restaurants_restaurant_thumb', col('restaurants_restaurant.thumb'))\
    .withColumn('restaurants_restaurant_url', col('restaurants_restaurant.url'))\
    .withColumn('restaurants_restaurant_user_rating', col('restaurants_restaurant.user_rating'))\
    .withColumn('restaurants_restaurant_user_rating_aggregate_rating', col('restaurants_restaurant_user_rating.aggregate_rating'))\
    .withColumn('restaurants_restaurant_user_rating_rating_color', col('restaurants_restaurant_user_rating.rating_color'))\
    .withColumn('restaurants_restaurant_user_rating_rating_text', col('restaurants_restaurant_user_rating.rating_text'))\
    .withColumn('restaurants_restaurant_user_rating_votes', col('restaurants_restaurant_user_rating.votes'))

df = df.drop('code', 'message', 'restaurants', 'status', 'restaurants_explode', 'restaurants_restaurant', 'restaurants_restaurant_R',
             'restaurants_restaurant_establishment_types', 'restaurants_restaurant_location', 'restaurants_restaurant_user_rating')

# df.show()

df.write.parquet('./p_files/', mode='overwrite')