# Risk of isolation increases the expected burden from sea-level rise
Please access the [paper here](https://www.nature.com/articles/s41558-023-01642-3) and [associated research briefing, here](https://www.nature.com/articles/s41558-023-01647-y).

An [interactive dashboard is available here](https://research.uintel.co.nz/slr-usa/).

## Citation: 
Logan, T. M., Anderson, M. J., & Reilly, A. C. (2023). Risk of isolation increases the expected burden from sea-level rise. Nature Climate Change, 1–6. https://doi.org/10.1038/s41558-023-01642-3

## Abstract
The typical displacement metric for sea-level rise adaptation planning is property inundation. However, this metric may underestimate risk as it does not fully capture the wider cascading or indirect effects of sea-level rise. To address this, we propose complementing it by considering the risk of population isolation: those who may be cut off from essential services. We investigate the importance of this metric by comparing the number of people at risk from inundation to the number of people at risk from isolation. Considering inundated roadways during mean higher high water tides in the coastal United States shows, although highly spatially variable, that the increase across the United States varies between 30% and 90% and is several times higher in some states. We find that risk of isolation may occur decades sooner than risk of inundation. Both risk metrics provide critical information for evaluating adaptation options and giving priority to support for at-risk communities.


# Data access

Data is available [here](https://drive.google.com/drive/folders/1nNo7sFqdyrJlSsOfNyWuD2Uj5_WZ9UQ9?usp=sharing).
Note that areas with no isolation/exposure are not included.

If the above link stops working, please contact the corresponding author via email or by raising a Git issue. 



# Summary of Destinations and Origins
### Blocks/origins
Use the blocks that are within the tracts (specified above).
Data from https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geodatabase-file.2016.html
See code `get_blocks.py`
Merge demographic with NHGIS data

Handy info on geo-identifiers https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html



### Destinations
Placed into SQL if they are within 5km of a county  # buffer distance as per Williams (2020)
Superamrkets located same method as 500cities

#### Schools
Private and public primary schools
https://hifld-geoplatform.opendata.arcgis.com/datasets/private-schools
https://hifld-geoplatform.opendata.arcgis.com/datasets/public-schools

#### Fire Stations
https://hifld-geoplatform.opendata.arcgis.com/datasets/fire-stations

#### Hospitals and Emergency Medical Services
Merge the Urgent Care Facilities and Hospital
https://hifld-geoplatform.opendata.arcgis.com/datasets/urgent-care-facilities
https://hifld-geoplatform.opendata.arcgis.com/datasets/hospitals

#### Pharmacies
https://hifld-geoplatform.opendata.arcgis.com/datasets/pharmacies

#### Supermarkets
OSM
