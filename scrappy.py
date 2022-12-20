import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
                "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding":"gzip, deflate, br",
                "Accept-Language":"en-US,en;q=0.9",
                "Connection":"keep-alive",
                "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36",
                "Cache-Control":"max-age=0, no-cache, no-store",
                "Upgrade-Insecure-Requests": "1"
    }

businessName = []
businessCategory = []
businessIntRating = []
businessTARating = []
businessFPRating = []

#Main
def main():
    for page in range(1, 3, 1):
        print(f'Page {page}')
        page = requests.get(f"https://www.yellowpages.com/los-angeles-ca/restaurants?page={page}", headers = HEADERS)
        page_content = page.content
        soup = BeautifulSoup(page.content, 'html.parser')
        getResName(soup,businessName)
        getCategory(soup,businessCategory)
        getIntRating(soup,businessIntRating)
        getTARating(soup,businessTARating)
        getFPRating(soup, businessFPRating)
    

#BusinessName
def getResName(soup,businessName):
    business_name = soup.find_all('a', {'class': 'business-name', 'href': True})
    for i,j in enumerate(business_name):
        try:
            value =j.find('span').text
            businessName.append(value)
        except:
            value = ""
            businessName.append(value)
            pass
    
#Category
def getCategory(soup, businessCategory):
    business_category = soup.find_all('div', class_ = 'categories')
    for category in business_category:
        try:
            tag = category.text
            businessCategory.append(tag)
        except:
            businessCategory.append("")
            pass

#Getting the ratings

#Internal Rating of the website
def getIntRating(soup, businessIntRating):
    business_internal_rating = soup.find_all(class_='rating hasExtraRating')
    for rating in business_internal_rating:
        rating = str(rating)
        businessIntRating.append(re.findall('(?<=result-rating )(\S*\w*\s*\w*)\"', rating))
        
#TripAdvisor Rating
def getTARating(soup, businessTARating):
    business_tripadvisor_rating = soup.find_all(class_='ratings')
    for rating in business_tripadvisor_rating:
        rating = str(rating)
        businessTARating.append(re.findall('(?<="rating":")(\w*\S\S)\"', rating))
        
#FP Rating
def getFPRating(soup, businessFPRating):
    busines_fp_rating = soup.find_all(span, class_ ='fs-rating')
    print(busines_fp_rating)
    for rating in busines_fp_rating:
        rating = str(rating)
        print(re.findall('(?<=data-foursquare=")(\w*\S\S)\"', rating))
        businessFPRating.append(re.findall('(?<=data-foursquare=")(\w*\S\S)\"', rating))




main()
print(businessFPRating)