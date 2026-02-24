from pyspark import pipelines as dp
#We are modernizing our Imperative Pipeline to Declarative
#We are using Lakeflow Ingestion (Auto Loader)
@dp.table(name="logistics_dp.logistics_schema.bronze_staff_data1")
def bronze_staff1_data():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/logistics_dp/logistics_schema/datalake/staff1/"))

@dp.table(name="logistics_dp.logistics_schema.bronze_staff_data2")
def bronze_staff2_data():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/logistics_dp/logistics_schema/datalake/staff2/"))


@dp.table(name="logistics_dp.logistics_schema.bronze_geotag_data1")
def bronze_geotag_data():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .load("/Volumes/logistics_dp/logistics_schema/datalake/geotag1/")
    )


@dp.table(name="logistics_dp.logistics_schema.bronze_shipments_data1")
def bronze_shipments_data():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/logistics_dp/logistics_schema/datalake/shipments1/")
            # .select(
            #     "shipment_id",
            #     "order_id",
            #     "source_city",
            #     "destination_city",
            #     "shipment_status",
            #     "cargo_type",
            #     "vehicle_type",
            #     "payment_mode",
            #     "shipment_weight_kg",
            #     "shipment_cost",
            #     "shipment_date"
            # )
    )
