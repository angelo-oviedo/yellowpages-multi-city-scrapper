# Yellow Pages Scraper

Sample of an ETL script which will take all data {EXTRACT}, clean and process as desired {TRANSFORM} and moves all data into a proper load format {LOAD}.

This project scrapes data from the yellow pages restaurant listings for major cities in the US and stores it in a database. The data is then prepared and cleaned for analysis, and several visualizations are created using the seaborn library. Finally, a recommendation is made to the business based on the data found in the database.


## Project Description

This project is for a digital services and support company that provides services to restaurants. The project involves scraping data from the yellow pages restaurant listings for major cities in the US, and storing it in a database. The data is then prepared and cleaned for analysis to identify potential prospects and new business.

### Business Viewpoint:

Our client is an aspiring business owner who wants to start a new venture in the restaurant industry. They are looking for insights on successful restaurant owners to better understand the market and create a competitive advantage. As a high-tech company, we have been approached to help our client obtain data on the most successful restaurant owners across the country.

### Technical Requirements:

--Web scraping tools such as Beautiful Soup and Selenium to collect data from restaurant review websites
--SQL database to store and organize data
--Python data analysis libraries such as Pandas, NumPy, and Matplotlib to clean and analyze data
--Machine learning libraries such as Scikit-Learn to identify patterns and trends in data

### Deliverables:

--A comprehensive report detailing the findings of the data analysis, including insights on the most successful restaurant owners and their common characteristics
--A dashboard or interactive visualization tool to present the analysis results in a user-friendly manner
--Clean and organized Python code that can be easily reused and modified for future projects

By delivering this project, we will help our client to make informed decisions that are backed by data and insights on the most successful restaurant owners across the country. Our proposed solution will provide a comprehensive database, analysis, and visualization of the data to ensure that our client can easily access the insights they need to make informed business decisions.

## Requirements

To run this project, you will need the following:

--Python 3.7 or higher
--The following Python libraries: requests, beautifulsoup4, pandas, psycopg2, sqlalchemy, seaborn
--An AWS database
--DBeaver or any other SQL client to connect to the database

## Usage

To run the project, follow these steps:

--Clone the repository.
--Install the required libraries using pip install -r requirements.txt.
--Set up the AWS database and configure the config.py file with the appropriate credentials.
--Run the yellow_pages_scraper.py file to scrape the data and store it in the database.
--Run the data_analysis.py file to create the visualizations and generate the recommendation.

## Credits

Credit to the restaurant business clients for their support and feedback on this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
