from distutils.log import error
import json
from bs4 import BeautifulSoup
import requests
import concurrent.futures
import logging

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def get_html_document(url):
  response = requests.get(url)
  return response.text


def get_symbols(url):
  symbols = []
  htmldocument = get_html_document(url)
  soup = BeautifulSoup(htmldocument, 'html.parser')

  tablerows = soup.findAll("tr")[1:]

  for tablerow in tablerows:
    try:
      symbols.append(tablerow.find("td").find("a").get_text())
    except:
      pass
  return symbols



def scrape_sequentially(symbols):
  symbol_and_stock_price = {}
  logging.info("Scraping sequentially: started")
  for idx, symbol in enumerate(symbols):
    logging.info(f"Scraping sequentially: Symbol {symbol} index {idx}")
    try:
      fin_url = f"https://finance.yahoo.com/quote/{symbol}"
      fin_soup = BeautifulSoup(get_html_document(fin_url), 'html.parser')
      value = fin_soup.find_all("fin-streamer")[18].get_text()
      symbol_and_stock_price[symbol] = value
      logging.info(f"Scraping sequentially: ended {symbol} = {value}")
    except:
      logging.warning(f"Scraping sequentially: ended {symbol}. Couldn't scrape")
  logging.info("Scraping sequentially: ended")
    
  
  with open("lab4/sequentially.json", "w") as f:
    f.write(json.dumps(symbol_and_stock_price))


def scrape_paralelly(symbols):
  symbol_and_stock_price = {}


  def thread_function(symbol):
    logging.info(f"Scraping parallely: Symbol {symbol}")
    try:
      fin_url = f"https://finance.yahoo.com/quote/{symbol}"
      fin_soup = BeautifulSoup(get_html_document(fin_url), 'html.parser')
      value = fin_soup.find_all("fin-streamer")[18].get_text()
      symbol_and_stock_price[symbol] = value
      logging.info(f"Scraping parallely: ended {symbol} = {value}")
    except:
      logging.warning(f"Scraping parallely: ended {symbol}. Couldn't scrape")

  logging.info("Scraping parallely: started")
  with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
    executor.map(thread_function, symbols)
  logging.info("Scraping parallely: ended")

  with open("lab4/parallely.json", "w") as f:
    f.write(json.dumps(symbol_and_stock_price))





if __name__ == "__main__":
  format = "%(asctime)s: %(message)s"
  logging.basicConfig(filename='lab4/.log', filemode='w', format=format, level=logging.INFO, datefmt="%H:%M:%S")
  symbols = get_symbols(URL)
  scrape_paralelly(symbols)
  scrape_sequentially(symbols)
