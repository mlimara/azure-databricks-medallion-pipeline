**Building a Seed Distributor Data Platform**

##Project Description

The Seed Distributor business is moving away from an old, manual system full of dirty data. With over 5 million addresses and 22 million orders, the old system cannot process data fast enough. To fix this, I built an automated Big Data pipeline using Azure Databricks. This creates a clean data foundation for fast, real-time analytics.
*Customer KPIs (Loyalty & Value)*: Tracks individual purchasing behavior—including total lifetime value and coupon usage—to help the marketing team identify and reward our most profitable VIP customers.
*Monthly KPIs (Business Growth)*: Provides a time-series view of order volumes and revenue trends month-over-month, giving executives a clear, high-level picture of overall business growth.
*Product KPIs (Inventory Performance)*: Analyzes the catalog to show which specific seeds generate the most revenue and sell the highest volumes, directly guiding future inventory and sales decisions.
*Regional KPIs (Logistics & Reach)*: Maps out order volumes by state and calculates average delivery speeds, helping the logistics team spot shipping bottlenecks and identify states with low market penetration.
This project successfully replaced an old, failing system with a modern, automated data pipeline. One major insight was that using a Dead Letter Queue to quarantine bad data is much safer for the business than just deleting it. Also, doing the heavy math early in the Gold layer was essential to make our final dashboards load instantly.  Finally, now that our data is perfectly clean, we can start using it for Artificial Intelligence. We can train Machine Learning models to predict future inventory needs and stop customers from leaving.
