import httpx
from bs4 import BeautifulSoup
import pandas as pd

SITE = 'https://www.findchips.com/search/'


def parse_item(url: str):
    """

    :param url:
    """
    item_dict = {}

    soup = BeautifulSoup(httpx.get(url).text, features="lxml")

    distributors = soup.find_all(class_='distributor-results')
    for distributor in distributors:
        if not distributor.find('td', class_='td-price').getText().split():
            continue
        item_dict['seller'] = ' '.join(distributor.findNext(class_='distributor-title').getText().split())

        subpositions = distributor.find_all(class_='row')
        for subposition in subpositions:
        # print(distributor.find('td', class_='td-price').getText())
        #     print(subposition)
            item_dict['name'] = ' '.join(
                subposition.find('td', class_='td-part first').find('a', class_='onclick').getText().split())
            item_dict['vendor'] = ' '.join(subposition.findNext('td', class_='td-mfg').getText().split())

            item_dict['MoQ'] = subposition.findNext('span', class_='additional-value', attrs={'data-title': 'Min Qty'})
            if item_dict['MoQ']:
                item_dict['MoQ'] = item_dict['MoQ'].getText()
            item_dict['PQ'] = subposition.findNext('span', class_='additional-value', attrs={'data-title': 'Package Mult.'})
            if item_dict['PQ']:
                item_dict['PQ'] = item_dict['PQ'].getText()

            prices = subposition.findNext('td', class_='td-price').getText().split()

            if 'See' in prices:
                prices.remove('See')
            if 'More' in prices:
                prices.remove('More')
            item_dict['prices'] = '[' + '; '.join(
                f"'{prices[i]}', '{prices[i + 1]}'" for i in range(0, len(prices), 2)) + ']'

            item_dict['in_stock'] = ' '.join(subposition.findNext('td', class_='td-stock').getText().split())
            item_dict['lead_time'] = subposition.findNext('span', class_='additional-value',
                                                          attrs={'data-title': 'Lead time'})
            if item_dict['lead_time']:
                item_dict['lead_time'] = item_dict['lead_time'].getText()
            item_dict['date_code'] = subposition.findNext('span', class_='additional-value',
                                                          attrs={'data-title': 'Date Code'})
            if item_dict['date_code']:
                item_dict['date_code'] = item_dict['date_code'].getText()
            item_dict['short_description'] = subposition.findNext(class_='td-description more').getText()
            item_dict['seller_url'] = f'https:{subposition.find('a', class_='onclick')['href']}'

            print(item_dict)


if __name__ == '__main__':
    to_search = 'BAV70WQ-7-F'
    request = f'{SITE}{to_search}?currency=USD'
    parse_item(request)
