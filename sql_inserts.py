import pandas as pd
df = pd.read_excel("StructuredFinal.xlsx")
df = df.replace('', pd.NA)# Replace empty strings with NaN
df.columns = [
        "latitude", "longitude", "Temp_pre_30", "Temp_pre_15", "Temp_pre_7", "Temp_cont",
        "Wind_pre_30", "Wind_pre_15", "Wind_pre_7", "Wind_cont",
        "Hum_pre_30", "Hum_pre_15", "Hum_pre_7", "Hum_cont",
        "Prec_pre_30", "Prec_pre_15", "Prec_pre_7", "Prec_cont",
        "fire_size", "state", "discovery_month", "putout_time",
        "disc_pre_year", "disc_pre_month", "stat_cause_descr", "row_id",
        "fire_name", "fire_size_class", "disc_clean_date", "cont_clean_date",
        "wstation_usaf"
    ]
df = df.drop_duplicates()
df.to_csv("structured_final.csv", index=False, na_rep='\\N')

