# -*- coding: utf-8 -*-

from googlemaps import GoogleMapsScraper
import argparse
import csv
from termcolor import colored


ind = {'most_relevant': 0, 'newest': 1, 'highest_rating': 2, 'lowest_rating': 3}

HEADER = ['id_review', 'caption', 'relative_date', 'review_date', 'retrieval_date',
          'rating', 'username', 'n_review_user', 'n_photo_user', 'url_user']
HEADER_W_SOURCE = HEADER + ['url_source']


def csv_writer(source_field, outpath):
    targetfile = open('data/' + outpath, mode='w', encoding='utf-8', newline='\n')
    writer = csv.writer(targetfile, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(HEADER_W_SOURCE if source_field else HEADER)
    return writer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google Maps reviews scraper.')
    parser.add_argument('--N', type=int, default=100, help='Number of reviews to retrieve')
    parser.add_argument('--i', type=str, default='urls.txt', help='Input file with target URLs')
    parser.add_argument('--o', type=str, default='output.csv', help='Output CSV filename (written to data/)')
    parser.add_argument('--sort_by', type=str, default='newest',
                        help='Sort order: most_relevant | newest | highest_rating | lowest_rating')
    parser.add_argument('--place', dest='place', action='store_true',
                        help='Extract place metadata instead of reviews')
    parser.add_argument('--debug', dest='debug', action='store_true',
                        help='Run with visible browser window (disables headless mode)')
    parser.add_argument('--source', dest='source', action='store_true',
                        help='Append source URL column to each row (useful for multi-URL runs)')
    parser.set_defaults(place=False, debug=False, source=False)

    args = parser.parse_args()

    if args.sort_by not in ind:
        print(f"Unknown sort_by value '{args.sort_by}'. Choose from: {list(ind.keys())}")
        raise SystemExit(1)

    writer = csv_writer(args.source, args.o)

    with GoogleMapsScraper(debug=args.debug) as scraper:
        with open(args.i, 'r') as urls_file:
            for url in urls_file:
                url = url.strip()
                if not url:
                    continue

                if args.place:
                    print(scraper.get_account(url))
                else:
                    error = scraper.sort_by(url, ind[args.sort_by])

                    if error == 0:
                        n = 0
                        while n < args.N:
                            print(colored(f'[Review {n}]', 'cyan'))
                            reviews = scraper.get_reviews(n)
                            if len(reviews) == 0:
                                break

                            for r in reviews:
                                row_data = list(r.values())
                                if args.source:
                                    row_data.append(url)
                                writer.writerow(row_data)

                            n += len(reviews)
