import awswrangler as wr
from urllib.parse import urlparse, parse_qs
from datetime import date


class AdobeAnalytics:
    def __init__(self, s3_bucket, filename):
        self.s3_bucket = s3_bucket
        self.filename = filename
    
    def calculate_revenue(self):
        search_engine_list = []
        search_keyword_list = []
        revenue_list = []

        # read tsv file
        df = wr.s3.read_csv(path=f's3://{self.s3_bucket}/{self.filename}', sep='\t')

        # sort df based on ip and datetime
        sort_df = df.sort_values(['ip', 'date_time'])

        # drop the columns that are not needed
        transformed_df = sort_df.drop(['hit_time_gmt', 'user_agent', 'geo_region', 'geo_country', 'geo_city', 'pagename'], axis=1)

        # set the df index to ip
        transformed_df.set_index('ip', inplace=True)

        # iterate the df rows, parse the referrer url to create new column search engine and search keyword
        for row in transformed_df.itertuples():
            referrer_url = row.referrer
            parsed_url = urlparse(referrer_url)
            search_engine = parsed_url.hostname
            search_engine_list.append(search_engine)
            qr = parse_qs(parsed_url.query)
            
            if qr:
                try:
                    search_keyword = qr['q'][0]
                except KeyError:
                    search_keyword = ""
            else:
                search_keyword = ""
            search_keyword_list.append(search_keyword)

            # split the product list column to create new column revenue
            event_list = row.event_list
            if event_list == 1.0:
                revenue = row.product_list.split(';')[3]
                revenue_list.append(revenue)
            else:
                revenue = 0
                revenue_list.append(revenue)

        # add all the 3 list values to df
        transformed_df['search_engine'] = search_engine_list
        transformed_df['search_keyword'] = search_keyword_list
        transformed_df['revenue'] = revenue_list

        # create revenue df where revenue is greater than zero
        revenue_df = transformed_df.drop(
            ['date_time', 'event_list', 'page_url', 'product_list', 'referrer', 'search_engine', 'search_keyword'], axis=1)
        revenue_df = revenue_df[revenue_df.revenue != 0]

        # create search df where both search engine and search keyword is not empty
        search_df = transformed_df.drop(['date_time', 'event_list', 'page_url', 'product_list', 'referrer', 'revenue'], axis=1)
        search_df = search_df[(search_df.search_engine != "") & (search_df.search_keyword != "")]

        # merge both df using ip
        merged_df = search_df.merge(revenue_df, left_index=True, right_index=True, how='inner')

        # drop the index and sort the df by revenue
        output_df = merged_df.reset_index(drop=True)
        output_df = output_df.sort_values(['revenue'], ascending=False)

        # rename the column names as per requirement
        output_df = output_df.rename({'search_engine': 'Search Engine Domain', 'search_keyword': 'Search Keyword', 'revenue': 'Revenue'},
                                     axis=1)
        print(output_df)

        # construct today's date as per requirement
        today = date.today()
        dte = today.strftime("%Y-%m-%d")

        # write tab delimited output file dropping index
        wr.s3.to_csv(output_df, path=f's3://{self.s3_bucket}/output/{dte}_SearchKeywordPerformance.tab', sep='\t', index=False)

        return output_df


def lambda_handler(event, context):
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        s3_file = record['s3']['object']['key']
        # create object and execute the function
        AdobeAnalytics(s3_bucket, s3_file).calculate_revenue()
