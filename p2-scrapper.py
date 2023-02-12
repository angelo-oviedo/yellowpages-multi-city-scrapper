import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from fake_useragent import UserAgent
import psycopg2
from sqlalchemy import create_engine
from DBCon import con2
import logging
import uuid

#This will create a log file that will log any errors that occur in the program
logging.basicConfig(level=logging.DEBUG,
                    filename='scraping.log',
                    filemode='w',
                    format='%(asctime)s %(levelname)s %(message)s')


#Function that connects to the database and inserts the data into the database
def runQuery(data, connection):
    #Here we connect to the database, importing our connection string from the DBCon.py file
    connection = connection
     
    #Here we move the data from the dataframe into the database table, if the table doesnt exist it will create it and if the data is already in the table it will replace it
    print("Transfering Data into Database...")
    con2 = data.to_sql(name='restaurant_data', con=connection, if_exists='replace', index=False)
    print("Transfer Complete")
    connection.commit()

    connection.close()

#Generating a random user agent to use in the headers of the request to avoid being blocked or rate limited by the website
ua = UserAgent()
HEADERS = {
                "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding":"gzip, deflate, br",
                "Accept-Language":"en-US,en;q=0.9",
                "Connection":"keep-alive",
                "User-Agent": ua.random,
                "Cache-Control":"max-age=0, no-cache, no-store",
                "Upgrade-Insecure-Requests": "1"
            }

#Here we create a dictionary for each column in the dataframe
businessName = {}
businessCategory = {}
businessIntRating = {}
businessIntRatCount = {}
businessTARating = {}
businessTARatCount = {}
businessFPRating = {}
businessWebsite = {}
businessMenu = {}
businessYears = {}
businessAmenities = {}
businessPhone = {}
businessCity = {}
businessState = {}
businessZCode = {}
businessAdress = {}
businessPriceRange = {}
businessTopComment = {}
businessID= {}

#Here we create a dataframe and add the columns to it
df = pd.DataFrame(columns=['businessName', 'businessCategory', 'businessIntRating', 'businessIntRatCount', 'businessTARating', 'businessTARatCount', 
                           'businessFPRating', 'businessWebsite', 'businessMenu', 'businessYears', 'businessAmenities', 'businessPhone', 'businessCity', 
                           'businessState', 'businessZCode', 'businessAdress', 'businessPriceRange', 'businessTopComment'])

#This is the list of cities that we will be scraping data from
cities = ["los-angeles-ca", "miami-fl", "phoenix-az", "las-vegas-nv", "san-antonio-tx", "houston-tx", "chicago-il", "dallas-tx", 
          "orlando-fl", "philadelphia-pa", "atlanta-ga", "oklahoma-city-ok", "indianapolis-in", "memphis-tn", "charlotte-nc", 
          "louisville-ky", "jacksonville-fl", "el-paso-tx", "detroit-mi", "denver-co", "milwaukee-wi", "columbus-oh", "saint-louis-mo", 
          "fort-worth-tx", "kansas-city-mo", "albuquerque-nm", "baltimore-md", "baton-rouge-la", "sacramento-ca", "fresno-ca", "austin-tx", 
          "nashville-tn", "tulsa-ok", "tucson-az", "tampa-fl", "birmingham-al", "bakersfield-ca", "new-york-ny", "cleveland-oh", "brooklyn-ny", 
          "san-diego-ca", "corpus-christi-tx", "salt-lake-city-ut", "cincinnati-oh", "fort-lauderdale-fl", "new-orleans-la", "knoxville-tn", 
          "columbia-sc", "bronx-ny"]

#Main function to get the data from the website
def getData(df):
    counter = 1
    for city in cities:
        print(f'City: {city}')
        for page in range(1, 31, 1):
            print(f'Page {page}')
            #This try except block is to catch any errors that may occur when trying to get the page such as  404 error if the page is not found, 
            # a 403 error if the request is forbidden, or a connection error if there is a problem connecting to the website
            try:
                page = requests.get(f"https://www.yellowpages.com/{city}/restaurants?page={page}", headers = HEADERS)
                page.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                print ("HTTP Error:",errh)
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)
            except requests.exceptions.RequestException as err:
                print ("Something Else:",err)
            page_content = page.content
            soup = BeautifulSoup(page_content, 'html.parser')
            main = soup.find_all('div', class_='search-results organic')
            for m in main:
                results = m.find_all("div", {"class": "result"})
                for id,result in enumerate(results):
                    
                    businessID[id] = "R-"+str(counter).zfill(3)
                    counter +=1
                    
                    if('business-name' in str(result)):
                        getResName(id, result, businessName)
                    else:
                        businessName[id] = None
                    if('categories' in str(result)):
                        getCategory(id, result, businessCategory)
                    else:
                        businessCategory[id] = None
                    if(len(result.find_all(class_='ratings')) > 0):
                        getIntRating(id, result, businessIntRating)
                    else:
                        businessIntRating[id] = None
                    if('data-tripadvisor' in str(result)):
                        getIntRatCount(id, result, businessIntRatCount)
                    else:
                        businessIntRatCount[id] = None
                    if(result.find_all(class_='ta-rating-wrapper') is not None):
                        getTARating(id, result, businessTARating)
                    else:
                        businessTARating[id] = None
                    if(result.find_all(class_='ta-rating-wrapper') is not None):
                        getTARatCount(id, result, businessTARatCount)
                    else:
                        businessTARatCount[id] = None
                    if(result.find_all(class_='fs-rating-wrapper') is not None):
                        getFPRating(id, result, businessFPRating)
                    else:
                        businessFPRating[id] = None
                    if(result.find_all(class_='track-visit-website') is not None):
                        getWebsite(id, result, businessWebsite)
                    else:
                        businessWebsite[id] = None
                    if(result.find_all(class_='menu') is not None):
                        getMenu(id, result, businessMenu)
                    else:
                        businessMenu[id] = None
                    if(result.find_all(class_='years-in-business') is not None):
                        getYOB(id, result, businessYears)
                    else:
                        businessYears[id] = None
                    if(result.find_all(class_='amenities') is not None):
                        getAmenities(id, result, businessAmenities)
                    else:
                        businessAmenities[id] = None
                    if(result.find_all(class_='phones phone primary') is not None):
                        getPhoneNumber(id, result, businessPhone)
                    else:
                        businessPhone[id] = None
                    if(result.find_all(class_='locality') is not None):
                        getCity(id, result, businessCity)
                    else:
                        businessCity[id] = None
                    if(result.find_all(class_='locality') is not None):
                        getState(id, result, businessState)
                    else:
                        businessState[id] = None
                    if(result.find_all(class_='locality') is not None):
                        getZCode(id, result, businessZCode)
                    else:
                        businessZCode[id] = None
                    if(result.find_all(class_='street-address') is not None):
                        getAdress(id, result, businessAdress)
                    else:
                        businessAdress[id] = None
                    if(result.find_all(class_='price-range') is not None):
                        getPriceRange(id, result, businessPriceRange)
                    else:
                        businessPriceRange[id] = None
                    if(result.find_all(class_='top-comment') is not None):
                        getComment(id, result, businessTopComment)
                    else:
                        businessTopComment[id] = None
            
                    #After going over the data points, we append the data to the dataframe before clearing the dictionaries
                    df = df.append({'businessName': businessName.get(id), 
                                    'businessCategory': businessCategory.get(id), 
                                    'businessIntRating': businessIntRating.get(id), 
                                    'businessIntRatCount': businessIntRatCount.get(id),
                                    'businessTARating': businessTARating.get(id),
                                    'businessTARatCount': businessTARatCount.get(id),
                                    'businessFPRating': businessFPRating.get(id),
                                    'businessWebsite': businessWebsite.get(id),
                                    'businessMenu': businessMenu.get(id),
                                    'businessYears': businessYears.get(id),
                                    'businessAmenities': businessAmenities.get(id),
                                    'businessPhone': businessPhone.get(id),
                                    'businessCity': businessCity.get(id),
                                    'businessState': businessState.get(id),
                                    'businessZCode': businessZCode.get(id),
                                    'businessAdress': businessAdress.get(id),
                                    'businessPriceRange': businessPriceRange.get(id),
                                    'businessTopComment': businessTopComment.get(id),
                                    'businessID': businessID.get(id)}, 
                                    ignore_index=True)
                                    
                    
                    
                    #We clear the dictionaries to avoid duplicates or to avoid appending data over the existing data whenever changing the page
                    businessName.clear()
                    businessCategory.clear()
                    businessIntRating.clear()
                    businessIntRatCount.clear()
                    businessTARating.clear()
                    businessTARatCount.clear()
                    businessFPRating.clear()
                    businessWebsite.clear()
                    businessMenu.clear()
                    businessYears.clear()
                    businessAmenities.clear()
                    businessPhone.clear()
                    businessCity.clear()
                    businessState.clear()
                    businessZCode.clear()
                    businessAdress.clear()
                    businessPriceRange.clear()
                    businessTopComment.clear()
    df = df.applymap(lambda x: None if x == [] else x)
 


    return df

#BusinessName
def getResName(id,result,businessName): 
    business_name = result.find('a', {'class': 'business-name', 'href': True})
    value = value = business_name.text
    businessName[id] = value
        
#Category
def getCategory(id,result, businessCategory):
    business_category = result.find('div', class_ = 'categories')
    tag = business_category.text
    tag = re.findall('[A-Z][^,-0]*', tag)
    businessCategory[id] = tag
                
#Getting the ratings

#Internal Rating of the website
def getIntRating(id, result, businessIntRating):
    business_internal_rating = result.find(class_='rating hasExtraRating')
    if business_internal_rating is not None:
        rating = str(business_internal_rating)
        value = re.findall('(?<=result-rating )(\S*\w*\s*\w*)\"', rating)
        print(value)
        #This mapping is here to convert the rating to a number from 1 to 5 in increments of 0.5
        if value == ["one"]:
            value = 1
        elif value == ["one half"]:
            value = 1.5
        elif value == ["two"]:
            value = 2
        elif value == ["two half"]:
            value = 2.5
        elif value == ["three"]:
            value = 3
        elif value == ["three half"]:
            value = 3.5
        elif value == ["four"]:
            value = 4
        elif value == ["four half"]:
            value = 4.5
        elif value == ["five"]:
            value = 5
            
        businessIntRating[id] = value
    else:
        businessIntRating[id] = None      
        
#Internal Rating count
def getIntRatCount(id, result, businessIntRatCount):
     business_internal_rat_count = result.find(class_='count')
     if business_internal_rat_count is not None:
        count = business_internal_rat_count.text
        businessIntRatCount[id] = count.replace("(", "").replace(")", "")
     else:
        businessIntRatCount[id] = None

#TripAdvisor Rating
def getTARating(id, result, businessTARating):
    business_tripadvisor_rating = result.find(class_='ratings')
    if business_tripadvisor_rating is not None:
        rating = str(business_tripadvisor_rating)
        value = re.findall('(?<="rating":")(\w*\S\S)\"', rating)
        if len(value)>0:
            value = value[0]
            value = value.replace("(", "").replace(")", "")
            businessTARating[id] = value
        else:
            businessTARating[id] = None
    else:
        businessTARating[id] = None

        
#TripAdvisor Rating count
def getTARatCount(id, result, businessTARatCount):
    business_tripadvisor_rating_count = result.find(class_='ratings')
    if business_tripadvisor_rating_count is not None:
        number = str(business_tripadvisor_rating_count)
        value = re.findall('(?<="count":")(\d*)\"', number)
        if len(value)>0:
            value = value[0]
            businessTARatCount[id] = value
        else:
            businessTARatCount[id] = None
    else:
        businessTARatCount[id] = None

    
#FP Rating
def getFPRating(id, result, businessFPRating):
    busines_fp_rating = result.find('div', class_='ratings')
    if busines_fp_rating is not None:
        value = busines_fp_rating.get('data-foursquare')
        if value is not None:
            #In the yellowpages the other ratings are in increments of 0.5 and in a scale of 0 to 5, 
            # but the FP rating is in a scale of 1 to 10. So we need to normalize it to the same scale
            value = float(value)
            scaled_value = (value - 1) / 2 + 1
            rounded_value = round(scaled_value * 2) / 2
            businessFPRating[id] = rounded_value
        else:
            businessFPRating[id] = None
    else:
        businessFPRating[id] = None

                
#Get the link for the business website
def getWebsite(id, result, businessWebsite):
    data = result.find("div", {"class": "links"})
    if 'Website' in str(data):
        businessWebsite[id] = data.find('a','track-visit-website').get("href") 
    else:
        businessWebsite[id] = None        
        
#Get the link to the menu of the restaurant
def getMenu(id, result, businessMenu):
        data = result.find("a", {"class": "menu"})
        if('"menus","listing_features"' in str(data)):
            businessMenu[id] = "www.yellowpages.com" + result.find('a', 'menu').get("href") 
        else:
            businessMenu[id] = None

#Get the years of business
def getYOB(id, result, businessYears):
    data = result.find('div', class_ = 'number')
    if data is not None:
        tag = data.text
        tag = int(tag.strip())
        businessYears[id] = tag
    else:
        businessYears[id] = None 
          
#Get the amenities of the restaurants
def getAmenities(id, result, businessAmenities):
    data = result.find('div', class_ = 'amenities-info')
    if data is not None:
        tag = data.text
        tag = re.findall('[A-Z][^A-Z]*', tag)
        businessAmenities[id] = tag
    else:
        businessAmenities[id] = None
                
#Get the business phone number
def getPhoneNumber(id, result, businessPhone):
    business_phone_number = result.find('div', class_ = 'phones phone primary')
    if business_phone_number is not None:
        tag = business_phone_number.text
        businessPhone[id] = tag
    else:
        businessPhone[id] = None
                
#Get the business geographical information

#Get the business city
def getCity(id, result, businessCity):
    data = result.find('div', class_ = 'locality')
    if data is not None:
        n = str(data)
        value = re.findall('(?<=locality">)(\w*\W\w*)', n)[0]
        businessCity[id] = value
    else:
        businessCity[id] = None
                
def getState(id, result, businessState):
    data = result.find('div', class_ = 'locality')
    if data is not None:
        n = str(data)
        value = re.findall('(?<=, )(\w*)', n)[0]
        businessState[id] = value
    else:
        businessState[id] = None
        
#Get the business zip code
def getZCode(id, result, businessZCode):
    data = result.find('div', class_ = 'locality')
    if data is not None:     
        n = str(data)
        value = re.findall('\d+', n)[0]
        businessZCode[id] = value
    else:
        businessZCode[id] = None
        
#Get the business adress
def getAdress(id, result, businessAdress):
    business_adress = result.find('div', class_ = 'street-address')
    if business_adress is not None:
        tag = business_adress.text
        businessAdress[id] = tag
    else:
        businessAdress[id] = None

#Get the business price range
def getPriceRange(id, result, businessPriceRange):
    business_price_range = result.find('div', class_ = 'price-range')
    if business_price_range is not None:    
        tag = business_price_range.text
        businessPriceRange[id] = tag
    else:
        businessPriceRange[id] = None

#Get the business top comment in the yellow pages
def getComment(id, result, businessTopComment):
    business_top_comment = result.find('p', class_ = 'body with-avatar')
    if business_top_comment is not None:
        tag = business_top_comment.text
        businessTopComment[id] = tag
    else:
        businessTopComment[id] = None

#Get the data for the dataframe
data = getData(df)
data.to_csv('p2_restaurant_data.csv', index=False)
print(len(cities))
runQuery(data, connection)
