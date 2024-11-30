import polars as pl

disasters = pl.read_csv("1970-2021_DISASTERS.csv", infer_schema=False).rename({"Disaster Type": "disasterType"})

## ANNUAL FREQUENCIES

# calculate frequency of each disaster type by year
annual_frequencies = disasters.select(
    ["Year", "disasterType"]
    ).group_by("Year", maintain_order=True).len()

# save file
annual_frequencies.write_csv("cleaned/annual_frequencies.csv")

## ANNUAL FREQUENCIES BY COUNTRY

# calculate frequency of each disaster type by year AND country
annual_country_freq = disasters.select(
    ["Year", "disasterType", "Country"]
    ).group_by(["Year", "Country", "disasterType"], maintain_order=True).len()

# consolidate least frequent disaster types into Other category
least_freq = annual_country_freq.group_by("disasterType").sum().sort("len")[:6].select("disasterType")
least_freq = least_freq.to_series().to_list()
annual_country_freq = annual_country_freq.with_columns(disasterType = pl.col("disasterType").replace(least_freq, ["Other"] * 6 ))

# save file
annual_country_freq.write_csv("cleaned/annual_country_freq.csv")