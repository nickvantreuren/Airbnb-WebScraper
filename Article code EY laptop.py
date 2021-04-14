# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 17:04:45 2021

@author: YU337KM
"""

# -*- coding: utf-8 -*-
"""
@author: Nick van Treuren
This script was created by Ruben Kerkhofs and enriched by Nick van Treuren for educational purposes and contains the code that was used
to create the blogpost on webscraping Airbnb data.
"""



"""
    Setting up your environment
"""
from bs4 import BeautifulSoup 
#pip install -U selenium
from selenium import webdriver
import pandas as pd
import numpy as np
import time
import requests
import re
from sklearn.feature_extraction.text import CountVectorizer
import random

 

"""
    Getting started
"""
def getPage(url):
    ''' returns a soup object that contains all the information 
    of a certain webpage'''
    result = requests.get(url)
    content = result.content
    return BeautifulSoup(content, features = "lxml")

    
def getRoomClasses(soupPage):
    ''' This function returns all the listings that can 
    be found on the page in a list.'''
    rooms = soupPage.findAll("div", {"class": "_8ssblpx"})
    result = []
    for room in rooms:
        result.append(room)
    return result


def getListingLink(listing):
    ''' This function returns the link of the listing'''
    return "http://airbnb.com" + listing.find("a")["href"]


def getID(listing):
    '''Retrns the unique ID of a listing'''
    Listing_ID = listing.find("a")["target"]
    return Listing_ID.split("listing_",1)[1]


def getListingTitle(listing):
    ''' This function returns the title of the listing'''
    return listing.find("meta")["content"]


def getTopRow(listing):
    ''' Returns the top row of listing information'''
    return listing.find("div", {"class": "_b14dlit"}).text


def getRoomInfo(listing):
    ''' Returns the guest information'''
    return listing.find("div", {"class":"_kqh46o"}).text


def getBasicFacilities(listing):
    ''' Returns the basic facilities'''
    try:
        output = listing.findAll("div", {"class":"_kqh46o"})[1].text.replace(" ","") #Speeds up cleaning
    except:
        output = []
    return output


def getListingPrice(listing):
    ''' Returns the price'''
    try:
        price_1 = listing.find("div", {"class":"_mjvmnj"}).text
    except:
        price_1 = ""
    try:
        price_2 = listing.find("div", {"class":"_1fwiw8gv"}).text
    except:
        price_2 = ""
    output = price_1 + " // " + price_2
    return output


def getListingRating(listing):
    ''' Returns the rating '''
    try:
        output = listing.find("span", {"class":"_10fy1f8"}).text
    except:
        output = "(-1)"
    return output

def getListingReviewNumber(listing):
    ''' Returns the number of reviews '''
    try: # Not all listings have reviews // extraction failed
        output = listing.find("span", {"class":"_a7a5sx"}).text
        output = output[2:-1]
    except:
        output = -1   # Indicate that the extraction failed -> can indicate no reviews or a mistake in scraping
    return output

def extractInformation(soupPage):
    ''' Takes all the information of a single page (thus multiple listings) and
    summarizes it in a dataframe'''
    listings = getRoomClasses(soupPage)
    IDs,titles, links, toprows, roominfos, basicfacilitiess, prices, ratings, reviews = [], [], [], [], [], [], [], [], []
    for listing in listings:
        IDs.append(getID(listing))
        titles.append(getListingTitle(listing))
        links.append(getListingLink(listing))
        toprows.append(getTopRow(listing))
        roominfos.append(getRoomInfo(listing))
        basicfacilitiess.append(getBasicFacilities(listing))
        prices.append(getListingPrice(listing))
        ratings.append(getListingRating(listing))
        reviews.append(getListingReviewNumber(listing))
    dictionary = {"ID": IDs, "title": titles, "toprow": toprows, "roominfo": roominfos, "facilities" : basicfacilitiess, "price": prices, "rating": ratings, "link": links, "reviewnumber": reviews}
    return pd.DataFrame(dictionary)
  
'''
    Scraping all listings for a given city
'''
def findNextPage(soupPage):
    ''' Finds the next page with listings if it exists '''
    try:
        nextpage = "https://airbnb.com" + soupPage.find("a", {"aria-label" : "Next"})["href"]
    except:
        nextpage = "no next page"
    return nextpage

def getPages(url):
    ''' This function returns all the links to the pages containing 
    listings for one particular city '''
    result = []
    while url != "no next page": 
        page = getPage(url)
        result = result + [page]
        url = findNextPage(page)
    return result

def extractPages(url):
    ''' This function outputs a dataframe that contains all information of a particular
    city. It thus contains information of multiple listings coming from multiple pages.'''
    pages = getPages(url)
    # Do for the first element to initialize the dataframe
    df = extractInformation(pages[0])
    # Loop over all other elements of the dataframe
    for pagenumber in range(1, len(pages)):
        df = df.append(extractInformation(pages[pagenumber]))
    return df


''' 
    Scraping all listings for a collection of cities
'''

def scrapeURLs(listofURLs):
    ''' This function scrapes all listings of the cities listed in a list together
    with their URLs'''
    print(listofURLs[0][0]) # Shows which city is being scraped
    # Do it for the first element in the list to initialize dataframe
    df = extractPages(listofURLs[0][1])
    df.loc[:, "search_city"] = listofURLs[0][0] # Add the city as a feature
    # loop over all the other elements in the list and append to dataframe
    for i in range(1, len(listofURLs)):
        print(listofURLs[i][0]) # Shows which city is being scraped
        newrows = extractPages(listofURLs[i][1])
        newrows.loc[:, "search_city"] = listofURLs[i][0] # Add the city as a feature
        df = df.append(newrows)
    return df


'''
    Scraping detailed information of rooms with beautifulsoup
'''

def getDescription(detailpage):
    ''' Returns the self written description of the host '''
    return detailpage.find("div", {"class": "_1y6fhhr"}).text

def getDetailedScores(detailpage):
    output = []
    try:
        reviewsection = detailpage.find("div", {"data-plugin-in-point-id" : "REVIEWS_DEFAULT"})
        scores = reviewsection.findAll(class_ = '_a3qxec')
        for i in range(0, 6):
            split = scores[i].text.split(".")
            output.append(float(split[0][-1] + "." + split[1]))
    except: # then we just don't want to pass any scores
        output = ["Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown" ]
    return output
 
    
def getHostInfo(detailpage):
    ''' Returns the name of the host and when they joined'''
    host_main_info      = detailpage.find("div", {"data-plugin-in-point-id" : "HOST_PROFILE_DEFAULT"})
    host_name           = host_main_info.find(class_ = "_f47qa6").text.split("Joined")[0]
    host_joined         = str("Joined" + host_main_info.find(class_ = "_f47qa6").text.split("Joined")[1])
    host_num_reviews    = host_main_info.find("span", {"class" : "_pog3hg"}).text
    try:
        host_superhost = str(host_main_info.select('._pog3hg')[1].get_text())
    except:
        host_superhost = "no superhost data found"
    return str(host_name + ";" + host_superhost + ";" + host_num_reviews + ";" + host_joined)


'''
    Using selenium for all other information
'''
def setupDriver(url, waiting_time = 3):
    ''' Initializes the driver of selenium'''
    PATH = r"C:\Users\YU337KM\OneDrive - EY\Documenten\03 Rest\MASTER THESIS\chromedriver.exe"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument("--lang=en")
    options.add_argument(f'user-agent={user_agent}')
    #options.add_argument("--window-size=1920,1080")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    #options.add_argument("--disable-extensions")
    #options.add_argument("--proxy-server='direct://'")
    #options.add_argument("--proxy-bypass-list=*")
    #options.add_argument("--start-maximized")
    #options.add_argument('--disable-gpu')
    #options.add_argument('--disable-dev-shm-usage')
    #options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(executable_path=PATH, options=options)
    driver.get(url)
    time.sleep(waiting_time) 
    return driver

def getJSpage(url):
    ''' Extracts the html of the webpage including the JS elements,
    output should be used as the input for all functions extracting specific information
    from the detailed pages of the listings '''
    driver = setupDriver(url)
    read_more_buttons = driver.find_elements_by_class_name("_1d079j1e")
    try:
        for i in range(2, len(read_more_buttons)):
            read_more_buttons[i].click()
    except:
        pass
    html = driver.page_source
    driver.close()
    return BeautifulSoup(html, features="lxml") 


def getAmenitiesPage(detailpage):
    ''' This code fetches the html of the webpage containing the information
     about the amenities that are available in the room'''
    amenities_section = detailpage.find("div", {"data-plugin-in-point-id" : "AMENITIES_DEFAULT"})
    link = amenities_section.find(class_ = "_13e0raay")["href"]
    driver = setupDriver("https://airbnb.com" + link, 5) # Amenitiespage is a link disguished as a button, this is why I need to do this
    html = driver.page_source
    driver.close()
    return BeautifulSoup(html, features="lxml")
   

    
def getAddis(url): 
    ''' This function is used to extract the html of the additional pages (detail page and amenities page)'''
    global first
    global scraped
    output = pd.DataFrame(columns=["details_page", "amenities_page", "link"])
    try:
        dp = getJSpage(url)
        dp_raw = str(dp)
        time.sleep(1.2)
        ap = getAmenitiesPage(dp)
        output.loc[0] = [dp_raw, str(ap), url]
        print("------------------------------VVV - PAGES FOUND!")
    except:
        output.loc[0] = [-1, -1, -1]
        print("------------------------------XXX - no pages found...")
    if first: # Ensures that the columns have the correct titles 
        output.to_csv('intermediate_results_par.csv', mode='a', header=True, index = False)
        first = False
    else:
        output.to_csv('intermediate_results_par.csv', mode='a', header=False, index = False) 
    scraped += 1
    print("Scraped: {}".format(scraped))
    return output
  

def getReviewPage(detailpage):
    ''' This code fetches the html of the webpage containing the information
     about the reviews that are given'''
    link = detailpage.find("div", {"data-plugin-in-point-id" : "REVIEWS_DEFAULT"})
    link = link.find(class_ = "_13e0raay")["href"]
    driver = setupDriver("https://airbnb.com" + link, 5) # Review page is a link disguished as a button, this is why I need to do this
    
    SCROLL_PAUSE_TIME = 0.9

    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
    # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    read_more_buttons = driver.find_elements_by_class_name("_ejra3kg")
    try:
        for i in range(1, len(read_more_buttons)):
            read_more_buttons[i].click()
    except:
        pass
    html = driver.page_source
    driver.close()
    return BeautifulSoup(html, features="lxml")

  
def getReviews(detailpage):
    ''' Returns a list of the featured reviews on the page '''
    global scraped_reviews
    sleeping_time = random.randint(100,1000)/1000
    try:
        time.sleep(sleeping_time)
        print("Waited {} seconds...".format(sleeping_time))
        scraped_reviews += 1
        soup = getReviewPage(detailpage)
        reviews = soup.findAll(class_ = "_1gjypya")
        output = ""
        for review in reviews:
            output += review.text + "****" #**-** can be used to split reviews later again
        print("------------------------------VVV - REVIEWS FOUND!")
    except:
        output = "Unknown"
        print("------------------------------XXX - NO REVIEWS FOUND...")
    print("Scraped reviews: {}".format(scraped_reviews))
    return output


def getAmenities(amenitiespage):
    amenities = amenitiespage.findAll(class_ = "_vzrbjl")
    output = ""
    for amenity in amenities:
        try:
            output += re.findall('[A-Z][^A-Z]*', amenity.text)[0] + "**-**" 
        except:
            print("Can't find Amenities...part 1")
            output += "Unknown"
        try:
            output += amenity.find("span", {"class" : "_krjbj"}).text + "**-**"
        except:
            pass
    return output


def getResponseInfo(detailpage):
    try:
        output = detailpage.find(class_ = "_jofnfy").text
    except:
        output = ""
    return output


'''
    Clean functions basic data frame extracted using only beautifulsoup
'''

def cleanFacilities(df): # Treating the facilities as a bag of words to create dummy variables
    df.loc[:, "facilities"] = df["facilities"].astype(str).str.replace("[","").str.replace("]","")
    vectorizer = CountVectorizer(decode_error = "ignore") 
    X = vectorizer.fit_transform(df.facilities)
    bag_of_words = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names())
    return pd.concat([df.reset_index(drop=True).drop("facilities", axis = 1), bag_of_words], axis=1)


def cleanTitle(df):
    df.loc[:, "name"] = df["title"].str.split(" null ", n = 0, expand = True)[0].str.replace("-", "")
    df.loc[:, "true_city/town"] = df["title"].str.split(" null ", n = 0, expand = True)[1].str.replace("-", "").str.strip()
    return df.drop("title", axis = 1)

def cleanTopRow(df):
    df.loc[:, 'roomtype'] = df["toprow"].str.split(" in ", n = 0, expand = True)[0] 
    df.loc[:, 'detailed_location'] = df["toprow"].str.split(" in ", n = 0, expand = True)[1] 
    return df.drop("toprow", axis = 1)

def cleanRoomInfo(df):
    df.loc[:, "guests"]     = df.loc[:, "roominfo"].str.split(" · ", n = 0, expand = True)[0].str.replace(" guests", "")
    df.loc[:, "bedrooms"]   = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[1]
    df.loc[:, "beds"]       = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[2].str.replace(" bed", "").str.replace("s", "")
    df.loc[:, "bathrooms"]  = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[3]
    df.loc[:, "guests"]     = pd.to_numeric(df.guests, errors = 'coerce')
    df.loc[:, "beds"]       = pd.to_numeric(df.beds, errors = 'coerce')
    df.loc[:, "bedrooms"]   = pd.to_numeric(df.bedrooms.str.split(" ", n = 0, expand = True)[0], errors = "ignore")
    df.loc[:, "bathrooms"]  = pd.to_numeric(df.bathrooms.str.split(" ", n = 0, expand = True)[0], errors = "ignore")
    return df.drop("roominfo", axis = 1)

def cleanPrice(df):
    try:
        df.loc[:, "pricepernight"] = df.loc[:, "price"].split("$")[1].split("/")[0]
    except:
        df.loc[:, "pricepernight"] = df.loc[:, "price"]
    return df


def cleanRating(df):
    try:
        df.loc[:, "score"] = df.loc[:, 'rating'].str.split(" ", n = 0, expand = True)[1]
        df.loc[:, "score"] = pd.to_numeric(df.score, errors = "coerce")
    except:
        df.loc[:, "score"] = -1
    return df.drop("rating", axis = 1)

def cleanReviewNumber(df):
    df.loc[:, "reviewnumber"] = df.loc[:, 'reviewnumber'].str.split(" ", n = 0, expand = True)[0]
    df.loc[:, "reviewnumber"] = pd.to_numeric(df.reviewnumber, errors = "coerce")
    return df

def clean(df):
    df = cleanTitle(df)
    #df = cleanFacilities(df)
    df = cleanTopRow(df)
    df = cleanRoomInfo(df)
    #df = cleanPrice(df)
    #df = cleanRating(df)
    #df = cleanReviewNumber(df)
    # Reorder columns
    col1 = df.pop('price')
    df = pd.concat([df.reset_index(drop=True), col1], axis=1)
    col2 = df.pop('reviewnumber')
    df = pd.concat([df.reset_index(drop=True), col2], axis=1) 
    col3 = df.pop('link')
    df = pd.concat([df.reset_index(drop=True), col3], axis=1) 
    return df


'''
    Clean functions data frame containing the html of the additional pages
'''

def cleanAmenities(df):
    df.loc[:, "amenities"] = df.amenities.replace(np.nan, '', regex=True)# fit_transform cannot handle missing values
    df.loc[:, "amenities"] = df.amenities.str.replace(" ", "_").str.replace("-", " ").str.replace("*", "") #split in two because of a python bug (https://stackoverflow.com/questions/3675144/regex-error-nothing-to-repeat)
    vectorizer = CountVectorizer(decode_error = "ignore") 
    X = vectorizer.fit_transform(df.amenities)
    bag_of_words = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names())
    return pd.concat([df.reset_index(drop=True).drop("amenities", axis = 1), bag_of_words], axis=1)


def cleanReviews(df): 
    df.loc[:, "reviews"]                = df.reviews.replace("‘", '', regex=True)
    df.loc[:, "reviews"]                = df.reviews.replace("’", '', regex=True)
    df.loc[:, "reviews"]                = df.reviews.replace(";", '', regex=True)
    df.loc[:, "reviews"]                = df.reviews.replace(np.nan, '', regex=True)# fit_transform cannot handle missing values
    df.loc[:, "reviews"]                = df.reviews.str.split("-")
    df.loc[:, "Cleanliness_score"]      = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[0])
    df.loc[:, "Accuracy_score"]         = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[1])
    df.loc[:, "Communication_score"]    = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[2])
    df.loc[:, "Location_score"]         = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[3])
    df.loc[:, "Check-in_score"]         = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[4])
    df.loc[:, "Value_score"]            = df.details_page.apply(lambda x: getDetailedScores(BeautifulSoup(x, features = "lxml"))[5])
    return df

def getResponseTime(string):
    if "Response time" in string:
        output = string[string.find("Response time") + 15:]
    else:
        output = "Unknown"
    return output

def getResponseRate(string):
    if "Response rate" in string:
        temp = string[string.find("Response rate") + 15:string.find("Response rate")+20] 
        output = ""
        for letter in temp:
            if letter in "0123456789":
                output += letter
    else:
        output = "Unknown"      
    return output

def getLanguages(string):
    if "Language" in string:
        if "Response" in string:
            output = string[10:string.find("Response")].strip()
        else:
            output = string[10:].strip()
    else:
        output = "Unknown"
    return output

def getHostJoined(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "HOST_PROFILE_DEFAULT"})
        output = output.find(class_ = "_1fg5h8r").text
        output = output[10:]
    except:
        output = "Unknown"
    return output

def getHostedby(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "HOST_PROFILE_DEFAULT"})
        output = output.find(class_ = "_14i3z6h").text
        output = output[10:]
    except:
        output = "Unknown host"
    return output

def getHostStory(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "HOST_PROFILE_DEFAULT"})
        output = output.find(class_ = "_1y6fhhr").text
    except:
        output = "Unknown story"
    return output
    

def getHostInfo2 (detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "HOST_PROFILE_DEFAULT"})
        try:
            var_1 = output.find(class_ = "_pog3hg").text
        except:
            var_1 = "Unknown var_1"
        try:
            var_2 = output.findAll(class_ = "_pog3hg")[1].text
        except:
            var_2 = "Unknown var_2"
        try:
            var_3 = output.findAll(class_ = "_pog3hg")[2].text
        except:
            var_3 = "Unknown var_3"
        all_vars = [var_1, var_2, var_3]
    except:
        all_vars = ["Unknown", "Unknown", "Unknown"]
    return all_vars
    

def getHostTotalReviews(detailpage):
    output = getHostInfo2(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "Reviews" in output_1:
        output = output_1[:-8]
    elif "Reviews" in output_2:
        output = output_2[:-8]
    elif "Reviews" in output_3:
        output = output_3[:-8]
    else:
        output = "Unknown amount of reviews"
    return output

def getHostIdentity(detailpage):
    output = getHostInfo2(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "Identity" in output_1:
        output = "1"
    elif "Identity" in output_2:
        output = "1"
    elif "Identity" in output_3:
        output = "1"
    else:
        output = "0"
    return output 
       
def getHostSuperhost(detailpage):
    output = getHostInfo2(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "Superhost" in output_1:
        output = "1"
    elif "Superhost" in output_2:
        output = "1"
    elif "Superhost" in output_3:
        output = "1"
    else:
        output = "0"
    return output 


def getThingsToKnow(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "POLICIES_DEFAULT"})
        try:
            var_1 = output.find(class_ = "_m9x7bnz").text
        except:
            var_1 = "Unknown var_1"
        try:
            var_2 = output.findAll(class_ = "_m9x7bnz")[1].text
        except:
            var_2 = "Unknown var_2"
        try:
            var_3 = output.findAll(class_ = "_m9x7bnz")[2].text
        except:
            var_3 = "Unknown var_3"
        all_vars = [var_1, var_2, var_3]
    except:
        all_vars = ["Unknown", "Unknown", "Unknown"]
    return all_vars

def getHouseRules(detailpage):
    output = getThingsToKnow(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "House rules" in output_1:
        output = output_1
    elif "House rules" in output_2:
        output = output_2
    elif "House rules" in output_3:
        output = output_3
    else:
        output = "Unknown House rules"
    return output

def getHealthANDSafety(detailpage):
    output = getThingsToKnow(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "Health" in output_1:
        output = output_1
    elif "Health" in output_2:
        output = output_2
    elif "Health" in output_3:
        output = output_3
    else:
        output = "Unknown Health and Safety"
    return output
       
def getCancelPolicy(detailpage):
    output = getThingsToKnow(detailpage)
    output_1 = output[0]
    output_2 = output[1]
    output_3 = output[2]
    if "Cancellation" in output_1:
        output = output_1
    elif "Cancellation" in output_2:
        output = output_2
    elif "Cancellation" in output_3:
        output = output_3
    else:
        output = "Unknown Cancellation policy"
    return output

def getHighlights(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "HIGHLIGHTS_DEFAULT"}).text
    except:
        output = "No Highlights section found"
    return output

##################################### EDIT #################################
def getLocation(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "TITLE_DEFAULT"})
        output = output.find(class_ = "_ngrlha").text
    except:
        output = ["NO TITLE_DEFAULT", "NO TITLE_DEFAULT", "NO TITLE_DEFAULT"]
    return output

##################################### ORIGINAL ##############################

def getLocation(detailpage):
    try:
        output = detailpage.find("div", {"data-plugin-in-point-id" : "TITLE_DEFAULT"})
    except:
        output = ["NO TITLE_DEFAULT", "NO TITLE_DEFAULT", "NO TITLE_DEFAULT"]
    else:
        try:
            output = output.find(class_ = "_5twioja").text
            output = output.split(",")
        except:
            try:
                output = output.findAll(class_ = "_nu65sd")[2].text
                output = output.split(",")
            except:
                output = ["Unknown", "Unknown", "Unknown"]
    return output

#############################################################################
def cleanHostedBy(df):
    df.loc[:, "hosted_by"] = df.details_page.apply(lambda x: getHostedby(BeautifulSoup(x, features = "lxml")))
    return df

def cleanHostStory(df):
    df.loc[:, "host_story"] = df.details_page.apply(lambda x: getHostStory(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "host_story"]                = df.host_story.replace("‘", '', regex=True)
    df.loc[:, "host_story"]                = df.host_story.replace("’", '', regex=True)
    df.loc[:, "host_story"]                = df.host_story.replace(";", '', regex=True)
    return df

def cleanResponseTime(df):
    df.loc[:, "response_info"] = df.response_info.replace(np.nan, '', regex=True)
    df.loc[:, "response_time"] = df.response_info.apply(lambda x: getResponseTime(x))
    return df

def cleanResponseRate(df):
    df.loc[:, "response_rate"] = df.response_info.apply(lambda x: getResponseRate(x))
    return df

def cleanLanguages(df):
    df.loc[:, "languages"] = df.response_info.apply(lambda x: getLanguages(x))
    df.loc[:, "languages"] = df.languages.str.split(",")
    return df

def cleanHostJoined(df):
    df.loc[:, "host_joined"] = df.details_page.apply(lambda x: getHostJoined(BeautifulSoup(x, features = "lxml")))
    return df
    
def cleanHostTotalReviews(df):
    df.loc[:, "host_total_reviews"] = df.details_page.apply(lambda x: getHostTotalReviews(BeautifulSoup(x, features = "lxml")))
    return df

def cleanHostIdentity(df):
    df.loc[:, "host_verified"] = df.details_page.apply(lambda x: getHostIdentity(BeautifulSoup(x, features = "lxml")))
    return df
    
def cleanHostSuperhost(df):
    df.loc[:, "host_superhost"] = df.details_page.apply(lambda x: getHostSuperhost(BeautifulSoup(x, features = "lxml")))
    return df

################################################################adjust#######
def cleanLocation(df):
    try:
        df.loc[:, "listing_location_toprow"]     = df.details_page.apply(lambda x: getLocation(BeautifulSoup(x, features = "lxml")))
    except:
        df.loc[:, "listing_location_toprow"]     = "Unknown, Unknown, Unknown"
    return df

################################################################original#####
#def cleanLocation(df):
#    try:
#        df.loc[:, "listing_city"]     = df.details_page.apply(lambda x: getLocation(BeautifulSoup(x, features = "lxml"))[0])
#    except:
#        df.loc[:, "listing_city"]     = "Unknown"
#    try:
#        df.loc[:, "listing_state"]    = df.details_page.apply(lambda x: getLocation(BeautifulSoup(x, features = "lxml"))[1])
#    except:
#        df.loc[:, "listing_state"]    = "Unknown"
#    try:
#        df.loc[:, "listing_country"]  = df.details_page.apply(lambda x: getLocation(BeautifulSoup(x, features = "lxml"))[2])
#    except:
#        df.loc[:, "listing_country"]  = "Unknown"
#    return df
#############################################################################

def cleanHouseRules(df):
    df.loc[:, "house_rules"] = df.details_page.apply(lambda x: getHouseRules(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "house_rules"]                = df.house_rules.replace("‘", '', regex=True)
    df.loc[:, "house_rules"]                = df.house_rules.replace("’", '', regex=True)
    df.loc[:, "house_rules"]                = df.house_rules.replace(";", '', regex=True)
    return df

def cleanHealthANDSafety(df):
    df.loc[:, "health_safety"] = df.details_page.apply(lambda x: getHealthANDSafety(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "health_safety"]                = df.health_safety.replace("‘", '', regex=True)
    df.loc[:, "health_safety"]                = df.health_safety.replace("’", '', regex=True)
    df.loc[:, "health_safety"]                = df.health_safety.replace(";", '', regex=True)
    return df

def cleanCancelPolicy(df):
    df.loc[:, "cancel_policy"] = df.details_page.apply(lambda x: getCancelPolicy(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "cancel_policy"]                = df.cancel_policy.replace("‘", '', regex=True)
    df.loc[:, "cancel_policy"]                = df.cancel_policy.replace("’", '', regex=True)
    df.loc[:, "cancel_policy"]                = df.cancel_policy.replace(";", '', regex=True)
    return df

def cleanHighlights(df):
    df.loc[:, "highlights"] = df.details_page.apply(lambda x: getHighlights(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "highlights"]                = df.highlights.replace("‘", '', regex=True)
    df.loc[:, "highlights"]                = df.highlights.replace("’", '', regex=True)
    df.loc[:, "highlights"]                = df.highlights.replace(";", '', regex=True)
    return df


def cleanResponseInfo(df):
    df = cleanHostedBy(df)
    # df = cleanHostStory(df)
    df = cleanResponseTime(df)
    df = cleanResponseRate(df)
    df = cleanLanguages(df)
    df = cleanHostJoined(df)
    df = cleanHostTotalReviews(df)
    df = cleanHostIdentity(df)
    df = cleanHostSuperhost(df)
    df = cleanLocation(df)
    df = cleanHouseRules(df)
    df = cleanHealthANDSafety(df)
    df = cleanCancelPolicy(df)
    df = cleanHighlights(df)
    return df.drop("response_info", axis = 1)

'''
    Scraper
'''
#############################################################################
################################## ORIGINAL #################################
def scraper(urls, sample_size = None, random_state = 4321):
    print('---------- Retrieving URLs...')
    df = scrapeURLs(urls)
    listing_amount = df.shape[0]
    print("---------- Number of lisitngs found: {}".format(listing_amount))
    df = df.drop_duplicates(subset=['ID'])
    df = clean(df)
    if sample_size is not None:
        df = df.sample(sample_size, random_state = random_state)
    #df_raw = df
    #df = df_raw
    df.to_csv('ALL_DATA 1.csv',index=False,header=True,encoding='utf8',sep='|')
    listing_amount = df.shape[0]
    print("---------- Number of lisitngs to scrape: {}".format(listing_amount))
    print('---------- Retrieving detail page & amenities page HTML...')
    #df3 = pd.DataFrame(columns=["details_page", "amenities_page", "link"])
    for url in df.link:
        sleeping_time = random.randint(500,1000)/1000
        time.sleep(sleeping_time)
        print("Waited {} seconds...".format(sleeping_time))
        getAddis(url)
    #df3.to_csv('ALL_PAGES.csv',index=False,header=True,encoding='utf8',sep='|')
    print('---------- Finished retrieving detail page & amenities page HTML...')
    df2 = pd.read_csv("intermediate_results_par.csv")
    df = df.merge(df2, on = "link")
    del df2
    df.to_csv('ALL_DATA 2.csv',index=False,header=True,encoding='utf8',sep='|')
    print('---------- Removing pages not found...')
    df = df[df.details_page != -1]
    
    print('---------- Retrieving reviews...')
    df.loc[:, 'reviews'] = df.details_page.apply(lambda x: getReviews(BeautifulSoup(x, features = "lxml")))
    print('---------- Retrieving response info...')
    df.loc[:, 'response_info'] = df.details_page.apply(lambda x: getResponseInfo(BeautifulSoup(x, features = "lxml"))) 
    print('---------- Retrieving amenities...')
    df.loc[:, "amenities"] = df.amenities_page.apply(lambda x: getAmenities(BeautifulSoup(x, features = "lxml")))        
    
    print('---------- Cleaning reviews...')
    df = cleanReviews(df)
    print('---------- Cleaning response info...')
    df = cleanResponseInfo(df) 
    print('---------- Cleaning amenities...')
    df = cleanAmenities(df)
    
    df = df.drop(columns=['details_page', 'amenities_page'])
    df.to_csv('ALL_DATA_FINAL.csv',index=False,header=True,encoding='utf8',sep='|')
    del df
    print('---------- SCRAPER COMPLETE :D')
    

#############################################################################
    
'''
    Cities in scope
'''
texarkana_AR      = "https://www.airbnb.com/s/Texarkana--AR--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Texarkana%2C%20AR%2C%20United%20States&place_id=ChIJAel3mxRtNIYRDp1ICFog2lU"
texarkana_TX      = "https://www.airbnb.com/s/Texarkana--TX--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&query=Texarkana%2C%20TX%2C%20United%20States&place_id=ChIJV2SgwjJBNIYRcFvbYZI8WJ0&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click"
ardmore_AL        = "https://www.airbnb.com/s/Ardmore--AL--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&query=Ardmore%2C%20AL%2C%20USA&place_id=ChIJrSgF6OhZYogRCL75dkJBcH0&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click"
ardmore_TN        = "https://www.airbnb.com/s/Ardmore--TN--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Ardmore%2C%20TN%2C%20United%20States&place_id=ChIJC5OeZQtRYogR61G9z4hlrJA" 
newPinecreek_CA   = "https://www.airbnb.com/s/New-Pine-Creek--CA--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=New%20Pine%20Creek%2C%20CA%2C%20USA&place_id=ChIJo0FPBDooylQRna-yOMJHB80"
newPineCreek_OR   = "https://www.airbnb.com/s/New-Pine-Creek--OR--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=New%20Pine%20Creek%2C%20OR%2C%20USA"
verdi_NV          = "https://www.airbnb.com/s/Verdi--NV--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Verdi%2C%20NV%2C%20United%20States&place_id=ChIJTd00u9lamYARm3rNNWmje7A"
verdi_CA          = "https://www.airbnb.com/s/Verdi--CA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=search_query"
delmar_DE         = "https://www.airbnb.com/s/Delmar--DE/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Delmar%2C%20DE&place_id=ChIJl7RMl1b7uIkRQbvMpVxKV84"
delmar_MD         = "https://www.airbnb.com/s/Delmar--MD/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=search_query"
marydel_MD        = "https://www.airbnb.com/s/Marydel--MD--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Marydel%2C%20MD%2C%20USA&place_id=ChIJ1y3rHOaAx4kRHCd-QCmRA-w"
marydel_DE        = "https://www.airbnb.com/s/Marydel--DE--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Marydel%2C%20DE%2C%20USA&place_id=ChIJUTRd222Ax4kRbA1TUnj7bHU"
augusta_GA        = "https://www.airbnb.com/s/Augusta--GA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Augusta%2C%20GA%2C%20United%20States&place_id=ChIJQaoLFpfQ-YgR5Auq4_2K5z4"
northAugusta_SC   = "https://www.airbnb.com/s/North-Augusta--SC--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=North%20Augusta%2C%20SC%2C%20United%20States&place_id=ChIJpcoDKKHM-YgRDKpxH-Y3bjY"
eastStLouis_IL    = "https://www.airbnb.com/s/East-Saint-Louis--IL--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=East%20Saint%20Louis%2C%20IL%2C%20USA&place_id=ChIJX3O1dUOs2IcR0MZMZ2Wq0Qg"
stLouis_MO        = "https://www.airbnb.com/s/Saint-Louis--MO--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Saint%20Louis%2C%20MO%2C%20USA&place_id=ChIJ-Y7t-qm02IcRW-C7IsrqOb4"
unionCity_IN      = "https://www.airbnb.com/s/Union-City--IN--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Union%20City%2C%20IN%2C%20USA&place_id=ChIJv3rKHtTfP4gRCnMJzqio2bM"
unionCity_OH      = "https://www.airbnb.com/s/Union-City--OH--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Union%20City%2C%20OH%2C%20USA&place_id=ChIJ482rccfCP4gRxYkgSGwhptY"
siouxCity_IA      = "https://www.airbnb.com/s/Sioux-City--IA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Sioux%20City%2C%20IA%2C%20United%20States&place_id=ChIJCVJtRzZ5kocRjExonssKmCE"
siouxCity_NE      = "https://www.airbnb.com/s/South-Sioux-City--NE--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=South%20Sioux%20City%2C%20NE%2C%20United%20States&place_id=ChIJN53lUVcHjocRTRq04-LmZMA"
fulton_KY         = "https://www.airbnb.com/s/Fulton--KY/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Fulton%2C%20KY&place_id=ChIJN2V8KsN7eYgRdiKuMvVki2A"
southFulton_TN    = "https://www.airbnb.com/s/South-Fulton--TN--USA/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=South%20Fulton%2C%20TN%2C%20USA&place_id=ChIJQYv-a7x7eYgR9oiXNMOxh-E"
eastGrandForks_MN = "https://www.airbnb.com/s/East-Grand-Forks--MN--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=East%20Grand%20Forks%2C%20MN%2C%20United%20States&place_id=ChIJSXi1EZKGxlIRbWjfG_M5KfA"
grandForks_ND     = "https://www.airbnb.com/s/Grand-Forks--ND--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Grand%20Forks%2C%20ND%2C%20United%20States&place_id=ChIJJV68WzWBxlIRWj1ffNiUaws"
westWendover_NV   = "https://www.airbnb.com/s/West-Wendover--NV--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=West%20Wendover%2C%20NV%2C%20United%20States&place_id=ChIJ9SrRPwonrIARVRFQzlheM_k"
wendover_UT       = "https://www.airbnb.com/s/Wendover--UT--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Wendover%2C%20UT%2C%20United%20States&place_id=ChIJW4IHDwojrIARQL_GQ-wlyYc"
anthony_NM        = "https://www.airbnb.com/s/Anthony--NM--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Anthony%2C%20NM%2C%20United%20States&place_id=ChIJz3RafwMB3oYRt18WKhzSbsM"
anthony_TX        = "https://www.airbnb.com/s/Anthony--TX--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Anthony%2C%20TX%2C%20United%20States&place_id=ChIJU6EpfCP-3YYRQh-9vFBDF9Q"
bristol_TN        = "https://www.airbnb.com/s/Bristol--TN--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Bristol%2C%20TN%2C%20United%20States&place_id=ChIJTYnr7iqdWogRVofY2v7yVz8"
bristol_VA        = "https://www.airbnb.com/s/Bristol--VA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Bristol%2C%20VA%2C%20United%20States&place_id=ChIJbda4S0Z0UIgR8dvXSHnGl_0"
bluefield_VA      = "https://www.airbnb.com/s/Bluefield--VA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Bluefield%2C%20VA%2C%20United%20States&place_id=ChIJx-URvwkxTogRFrTst0ShYFo"
bluefield_WV      = "https://www.airbnb.com/s/Bluefield--WV--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Bluefield%2C%20WV%2C%20United%20States&place_id=ChIJGZu83XY_TogRqTDGMTz4r80"
jerseyCity_NJ     = "https://www.airbnb.com/s/Jersey-City--NJ--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Jersey%20City%2C%20NJ%2C%20United%20States&place_id=ChIJ3a-_JdJQwokR2SXNohPwSQI"
newYork_NY        = "https://www.airbnb.com/s/New-York--NY--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=New%20York%2C%20NY%2C%20United%20States&place_id=ChIJOwg_06VPwokRYv534QaPC8g"
lanet_AL          = "https://www.airbnb.com/s/Lanett--AL/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Lanett%2C%20AL&place_id=ChIJF75cLZajjIgRGbJ3mlsI0uk"
westPoint_GA      = "https://www.airbnb.com/s/West-Point--GA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=West%20Point%2C%20GA%2C%20United%20States&place_id=ChIJv6gFQzukjIgRJ8J5Ov89xRk"
phenixCity_AL     = "https://www.airbnb.com/s/Phenix-City--AL--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Phenix%20City%2C%20AL%2C%20United%20States&place_id=ChIJcQGiemjajIgRogsVCs0GYNQ"
columbus_GA       = "https://www.airbnb.com/s/Columbus--GA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Columbus%2C%20GA%2C%20United%20States&place_id=ChIJ9TE1WBvNjIgRYQPh7qlQjHI"
greenwich_CT      = "https://www.airbnb.com/s/Greenwich--CT--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Greenwich%2C%20CT%2C%20United%20States&place_id=ChIJly5MDxGYwokR4jAEYCeuMQg"
portChester_NY    = "https://www.airbnb.com/s/Port-Chester--NY--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Port%20Chester%2C%20NY%2C%20United%20States&place_id=ChIJMy_3YkKXwokRfDStiO8v75A"
washington_DC     = "https://www.airbnb.com/s/Washington--DC--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Washington%2C%20DC%2C%20United%20States&place_id=ChIJW-T2Wt7Gt4kRKl2I1CJFUsI"
washington_MD     = "https://www.airbnb.com/s/Washington--MD--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Washington%2C%20MD%2C%20United%20States&place_id=ChIJ5W-Kkev3yYkRRi6fquUOrYE"
chicago_IL        = "https://www.airbnb.com/s/Chicago--IL--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Chicago%2C%20IL%2C%20United%20States&place_id=ChIJ7cv00DwsDogRAMDACa2m4K8"
hammond_IN        = "https://www.airbnb.com/s/Hammond--IN--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Hammond%2C%20IN%2C%20United%20States&place_id=ChIJVf0uL_PeEYgRCBxieTCVQjs"
camden_NJ         = "https://www.airbnb.com/s/Camden--NJ--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Camden%2C%20NJ%2C%20United%20States&place_id=ChIJQwA2LhnJxokRhoaToXZr498"
philadelphia_PA   = "https://www.airbnb.com/s/Philadelphia--PA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Philadelphia%2C%20PA%2C%20United%20States&place_id=ChIJ60u11Ni3xokRwVg-jNgU9Yk"
portland_OR       = "https://www.airbnb.com/s/Portland--OR--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Portland%2C%20OR%2C%20United%20States&place_id=ChIJJ3SpfQsLlVQRkYXR9ua5Nhw"
vancouver_WA      = "https://www.airbnb.com/s/Vancouver--WA--United-States/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&checkin=2021-05-01&checkout=2021-05-08&source=structured_search_input_header&search_type=autocomplete_click&query=Vancouver%2C%20WA%2C%20United%20States&place_id=ChIJ-RRZyGOvlVQR8-ORLBHVVoQ"


urls = [
        ["Texarkana, Arkansas", texarkana_AR],
        ["Texarkana, Texas", texarkana_TX], 
        ["Ardmore, Alabama", ardmore_AL], 
        ["Ardmore, Tennessee", ardmore_TN],
        ["New Pine Creek, California", newPinecreek_CA],
        ["New Pine Creek Oregon", newPineCreek_OR],
        ["Verdi, Nevada", verdi_NV],
        ["Verdi, California", verdi_CA],
        ["Delmar, Delaware", delmar_DE],
        ["Delmar, Maryland", delmar_MD],
        ["Marydel, Maryland", marydel_MD], 
        ["Marydel, Delaware", marydel_DE],
        ["Augusta, Georgia", augusta_GA],
        ["North Augusta, Soutch Carolina", northAugusta_SC],
        ["East St. Louis, Illinois", eastStLouis_IL],
        ["St. Louis, Missouri", stLouis_MO],
        ["Union City, Indiana", unionCity_IN],
        ["Union City, Ohio", unionCity_OH],
        ["Sioux City, Iowa", siouxCity_IA],
        ["Sioux City, Nebreska", siouxCity_NE],
        ["Fulton, Kentucky", fulton_KY],
        ["South Fulton, Tennessee", southFulton_TN],
        ["East Grand Forks, Minnesota", eastGrandForks_MN],
        ["Grand Forks, North Dakota", grandForks_ND],
        ["West Wendover, Nevada", westWendover_NV],
        ["Wendover, Utah", wendover_UT],
        ["Anthony, New Mexico", anthony_NM],
        ["Anthony, Texas", anthony_TX],
        ["Bristol, Tennessee", bristol_TN],
        ["Bristol, Virginia", bristol_VA],
        ["Bluefield, Virginia", bluefield_VA],
        ["Bluefield, West Virginia", bluefield_WV],
        ["Jersey City, New Jersey", jerseyCity_NJ],
        ["New York, New York", newYork_NY],
        ["Lanet, Alabama", lanet_AL], 
        ["West Point, Georgia", westPoint_GA],
        ["Phenix City, Alabama", phenixCity_AL],
        ["Columbus, Gorgia", columbus_GA],
        ["Greenwich, Conneticut", greenwich_CT],
        ["Port Chester, New York", portChester_NY],
        ["Washington, District Columbia", washington_DC],
        ["Washington, Maryland", washington_MD],
        ["Chicago, Illinois", chicago_IL],
        ["Hammond, Indiana", hammond_IN],
        ["Camden, New Jersey", camden_NJ],
        ["Philadelphia, Pennsylvenia", philadelphia_PA],
        ["Portland, Oregon", portland_OR],
        ["Vancouver, Washington", vancouver_WA],
        ]


urls = [
        ["Texarkana, Arkansas", texarkana_AR],
        ["Texarkana, Texas", texarkana_TX],
        ]


'''
    Running te scraper 
'''

first = True
scraped = 0
scraped_reviews = 0
df3_final = scraper(urls, sample_size=3)

