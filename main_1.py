import time
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import aiofiles


async def fetch(session, url):
    response = await session.get(url)
    response.raise_for_status()
    return response.text


async def fetch_binary(session, url):
    response = await session.get(url)
    response.raise_for_status()
    return response.content


async def parse_items_urls(session, group_url: str) -> list[str]:
    """
    Парсит ссылки на товары со страницы группы товаров
    :param group_url:
    """
    items_urls = []
    response = await fetch(session, group_url)
    soup = BeautifulSoup(response, features="lxml")

    dirty_urls = soup.find_all('a', class_='item-info__fullLink')

    for url in dirty_urls:
        item_url = url.get("href")
        items_urls.append(f'https://www.symmetron.ru{item_url}')

    return items_urls


async def parse_item(session, item_url: str):
    item_dict = {}
    response = await fetch(session, item_url)
    soup = BeautifulSoup(response, features="lxml")
    group = soup.find_all(itemprop='name')
    group_string = ';'.join(item.getText() for item in group[1:-1])
    # for i in group[1:-1]:
    #     group_string+=i.getText())
    item_dict['group'] = group_string
    item_dict['name'] = group[-1].getText()
    try:
        pic_url = soup.find_all('img')[2].get('src')
        pic_file_name = f'{item_dict["name"]}.png'.replace('/', '').replace('\\', '')
        pic_content = await fetch_binary(session, pic_url)
        async with aiofiles.open(pic_file_name, 'wb') as file:
            await file.write(pic_content)
        item_dict['pic_file'] = pic_file_name
    except ValueError:
        item_dict['pic_file'] = ''
    try:
        doc_url = soup.find_all('a', class_='document-element__name')[0].get("href")
        doc_content = await fetch_binary(session, doc_url)
        doc_file_name = f'{item_dict["name"]}.pdf'
        async with aiofiles.open(doc_file_name, 'wb') as file:
            await file.write(doc_content)
        item_dict['doc_file'] = f'{item_dict['name']}.pdf'
    except IndexError:
        item_dict['doc_file'] = ''
    item_dict['descrition'] = soup.find('section', class_='detail').getText()

    specs_titles = soup.find_all('span', class_='spec-item__name')
    specs_values = soup.find_all('span', class_='spec-item__value')
    if len(specs_titles) == len(specs_values):
        for i in range(len(specs_titles)):
            spec_title = ' '.join(specs_titles[i].getText().split())
            item_dict[f'{spec_title}'] = f'{specs_values[i].getText().strip()}'

    print(item_dict)


async def read_xlsx():
    df = pd.read_excel('to_parse.xlsx')
    links = df['links'].to_list()
    return links


async def main():
    tic = time.perf_counter()

    timeout = httpx.Timeout(600.0, read=300.0, write=300.0, connect=100.0)
    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as session:
        group_urls = await read_xlsx()
        tasks = []
        for group_url in group_urls:
            item_urls_list = await parse_items_urls(session, group_url)
            print(item_urls_list)
            for url in item_urls_list:
                tasks.append(parse_item(session, url))
        await asyncio.gather(*tasks)

    toc = time.perf_counter()
    print(f"{toc - tic:0.4f} seconds")

if __name__ == '__main__':
    asyncio.run(main())
