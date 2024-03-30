
This project expands upon my previous work, which involved gathering weather data for Boston to develop a simplified dashboard for a transportation company serving the city.

The current initiative focuses on creating a system to assist in resource allocation for managing upcoming winter storms, specifically tailored for the Massachusetts Bay Transportation Authority (MBTA). This system provides comprehensive forecasts based on various weather factors that impact road and environmental conditions. By employing machine learning models and other analytical methods, it delivers visualizations to MBTA decision-makers, illustrating the magnitude and timing of weather events on an hourly basis. Integrated into existing storm planning processes, including overall storm level determination, the system underwent successful testing during the 2021-2022 period and is now undergoing full implementation.

Due to confidentiality concerns, a simplified version of the dashboard was utilized. Please refer to the attached data description for the final dataset.

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/f317d71a-cf3c-41b4-ba59-cc8ddffbe75a)

I opted for batch processing, scheduling Mage to execute the ETL process at 10:15 AM EST daily (2:00 AM UTC). The data collection from the NOAA weather API is managed by Mage, which triggers the API on a daily basis and imports the acquired data into Google Cloud Platform Buckets. Additionally, BigQuery is utilized to partition the dataset by validtime.

For detailed information on the codes pertaining to the data extraction, transformation, and loading (ETL) process, please refer to the attached documentation.

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/5524b8eb-d92e-43cd-81d5-138c4db986f1)

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/41117010-c49e-451d-9438-5a3c5a4f23c1)

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/943c5177-b5bd-439d-a91b-3e74deb9960c)


To enhance the storm severity rating, we implemented a rating system that involves calculations on the current data table and visualization of the results as a radar chart. To accomplish this objective, I utilized PySpark to read data from Google Cloud Platform (GCP) buckets, performed calculations to determine the indicator levels, and then reloaded the updated table back into the buckets as output.
Radar Chart example:

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/eb69e36c-0f13-46e7-9729-936280a6bd94)


To construct the dashboard, I generated datasets containing the completed tables in BigQuery and imported them into Looker. Additionally, I scheduled BigQuery to run at 10:30 PM EST and Looker to run at 11:00 PM EST. This ensures that the data is updated and available for visualization on the Looker dashboard.
PySpark codes please see attachment.

![image](https://github.com/TerryLiu207/DE-homework/assets/157868320/84b87c60-f54d-4ed3-a1bd-3adc570e5d20)


