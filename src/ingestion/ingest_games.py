from maxpreps_scraper.scraper import MaxPrepsScraper

scraper = MaxPrepsScraper()

rankings_df = scraper.get_rankings(state = 'de', sport = 'football', year = '23-24')

print(rankings_df.head())