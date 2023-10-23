### Sales Analysis Technical Exercise
The following artifacts were produced for a technical exercise while interviewing for a Senior Data Analyst role.  I was provided with a question about a decrease in sales along with a Mode workbook from a theoretical junior analyst. The deliverables included modified and DRY code, SQL tips to the Junior Analyst, data viz, slide deck, and an email to an executive explaining why sales results appeared down in the most recent quarter.
 
# SQL tips for junior analyst: 
1. I opted to condense the queries into one. Rather than having separate queries for each
   stratification, this single query can be used to aggregate across parameters. The BI tool
   utilizes measures to do this. For example, by creating a measure that is the sum(sales), we
   can determine the sales by category or customer type. We don't need to do that aggregation
   in SQL and have separate tables for category/customer type.

2. I've removed trailing commas and added leading commas. This is subjectively easier to read, 
   but can also make it more efficient if you need to comment out certain fields. The distinction 
   between leading and trailing commas is generally defined by a style guide. There is ample debate
   about which is better, but regardless of which is used, it's important to stay consistent.

3. I've replaced the hard-coded date logic with functions (date_trunc). Utilizing the date datatype
   allows for better functionality within the BI tool. For example, it will understand that months
   should be organized chronologically rather than alphabetically (if it were left as a string)

4. Refined the cross join by simplifying the select statement and adding the sales. 

5. In the joins, I added aliases and replaced using with on, which explicitly states the join criteria 
   improving interpretability. 


## Sales Analysis Visualizations
This are image exports of visualizations I produced in Mode and included in a slide deck in order to provide recommendations on where marketing should focus efforts to increase sales. 

![Sales Analysis Page 1](/SQL/Viz/page_1.png)
![Sales Analysis Page 2](/SQL/Viz/page_2.png)
![Sales Analysis Page 3](/SQL/Viz/page_3.png)
![Sales Analysis Page 4](/SQL/Viz/page_4.png)
![Sales Analysis Page 5](/SQL/Viz/page_5.png)
