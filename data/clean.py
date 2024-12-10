import polars as pl

disasters = pl.read_csv("raw/1970-2021_DISASTERS.csv", infer_schema=False).rename({"Disaster Type": "disasterType"})

## ANNUAL FREQUENCIES BY REGION

# calculate frequency of each disaster type by year AND country
regional_freq = disasters.select(
    ["Year", "Region","disasterType"]
    ).group_by(["Year","Region", "disasterType"], maintain_order=True).len()

# group least frequent disaster types into "other category"
least_freq = regional_freq.group_by("disasterType").sum().sort("len")[:6].select("disasterType")
least_freq = least_freq.to_series().to_list()
regional_freq = regional_freq.with_columns(disasterType = pl.col("disasterType").replace(least_freq, ["Other"] * 6 ))

# regroup to ensure there are not multiple others within one region/year
regional_freq = regional_freq.group_by(["Year","Region", "disasterType"], maintain_order=True).len()

regional_freq.write_csv("cleaned/regional_frequencies.csv")

## ANNUAL FREQUENCIES 

# consolidate least frequent disaster types into Other category
annual_frequencies = disasters.with_columns(\
    disasterType = pl.col("disasterType").replace(least_freq, ["Other"] * 6 ))

# calculate frequency of each disaster type by year
annual_frequencies = annual_frequencies.select(
    ["Year", "disasterType"]
    ).group_by(["Year", "disasterType"], maintain_order=True).len()


#create unique key for each row
annual_frequencies = annual_frequencies.with_columns(pl.concat_str([pl.col("Year"), pl.col("disasterType")]).alias("compkey"))

# save file
annual_frequencies.write_csv("cleaned/annual_frequencies.csv")

print("clean.py executed")

## DISASTER COSTS
disaster_costs = pl.read_csv("raw/disaster_costs.csv").rename(
        {"Entity" : "disasterType", "Total economic damages" : "economicdamage"})
disaster_costs = disaster_costs.filter(~pl.col("disasterType").str.contains("All disasters"))
disaster_costs = disaster_costs.with_columns(disasterType = pl.col("disasterType").replace(least_freq, ["Other"] * 6 ))
disaster_costs = disaster_costs.filter(pl.col("Year") >= 1970).filter(pl.col("Year") <= 2021)
disaster_costs = disaster_costs.group_by(["Year", "disasterType"]).sum()


disaster_costs.write_csv("cleaned/disaster_costs.csv")