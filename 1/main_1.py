import logging
import os.path
import time
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import aiofiles

logging.basicConfig(level=logging.INFO)


async def fetch(session: httpx.AsyncClient, url: str) -> str:
    """ Асинхронное подключение для получения HTML-страницы """
    try:
        response = await session.get(url)
        response.raise_for_status()
        return response.text
    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e}")
    except httpx.ConnectTimeout:
        print(f"Timeout error for {url}")


async def fetch_binary(session: httpx.AsyncClient, url: str) -> bytes:
    """ Асинхронное подключение для скачивания файлов"""
    try:
        response = await session.get(url)
        response.raise_for_status()
        return response.content
    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e}")
    except httpx.ConnectTimeout:
        print(f"Timeout error for {url}")


async def parse_items_urls(session: httpx.AsyncClient, group_url: str) -> list[str]:
    """
    Парсит ссылки на товары со страницы группы товаров
    :param group_url: url на группу
    """
    items_urls = []
    response = await fetch(session, group_url)
    if not response:
        print('Ошибка при парсинге ссылок на позиции')
        return []
    soup = BeautifulSoup(response, features="lxml")
    for url in soup.find_all('a', class_='item-info__fullLink'):
        item_url = url.get("href")
        items_urls.append(f'https://www.symmetron.ru{item_url}')
    return items_urls


async def parse_item(session, item_url: str):
    """
    Парсит страницу позиции товара
    :param session:
    :param item_url: url позиции
    """
    item_dict = {}
    response = await fetch(session, item_url)
    if not response:
        print('Ошибка парсинга позиции')
        return
    soup = BeautifulSoup(response, features="lxml")
    group = soup.find_all(itemprop='name')
    group_string = ';'.join(item.getText() for item in group[1:-1])

    item_dict['group'] = group_string
    item_dict['name'] = group[-1].getText()
    try: # парсим ссылку на картинку и скачиваем его
        pic_url = soup.find_all('img')[2].get('src')
        pic_file_name = f'{item_dict["name"]}.png'.replace('/', '').replace('\\', '')
        pic_content = await fetch_binary(session, pic_url)
        async with aiofiles.open(f'output/{pic_file_name}', 'wb') as file:
            await file.write(pic_content)
        item_dict['pic_file'] = pic_file_name
    except ValueError:
        item_dict['pic_file'] = ''
    try:  # парсим ссылку на doc file и скачиваем его
        doc_url = soup.find_all('a', class_='document-element__name')[0].get('href')
        doc_content = await fetch_binary(session, doc_url)
        doc_file_name = f'{item_dict['name']}.pdf'
        async with aiofiles.open(f'output/{doc_file_name}', 'wb') as file:
            await file.write(doc_content)
        item_dict['doc_file'] = f'{item_dict['name']}.pdf'
    except IndexError:
        item_dict['doc_file'] = ''
    item_dict['description'] = soup.find('section', class_='detail').getText()

    # парсим характеристики
    specs_titles = soup.find_all('span', class_='spec-item__name')
    specs_values = soup.find_all('span', class_='spec-item__value')
    if len(specs_titles) == len(specs_values):
        for i in range(len(specs_titles)):
            spec_title = ' '.join(specs_titles[i].getText().split())
            item_dict[f'{spec_title}'] = f'{specs_values[i].getText().strip()}'
    else:
        print('Ошибка парсинга характеристик')
    return item_dict


async def read_xlsx(file_name: str) -> list:
    """
    Чтение xlsx файла с ссылками на группы позиций
    :return:
    """
    df = pd.read_excel(file_name)
    return df['links'].to_list()


async def write_to_xlsx(items_list, file_name: str='items.xlsx'):
    """
    Запись xlsx файла
    :return:
    """
    df = pd.DataFrame(items_list)
    df.to_excel(file_name, index=False)


async def main():
    group_urls = await read_xlsx('to_parse.xlsx')
    if not os.path.exists('output'):
        os.mkdir('output')

    timeout = httpx.Timeout(300.0, read=100.0, write=100.0, connect=100.0)
    # limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    async with httpx.AsyncClient(timeout=timeout) as session:

        tasks = []
        items_list = []
        for group_url in group_urls:
            item_urls_list = await parse_items_urls(session, group_url)
            for url in item_urls_list:
                tasks.append(parse_item(session, url))
        results = await asyncio.gather(*tasks)

        for result in results:
            if result:
                items_list.append(result)

        await write_to_xlsx(items_list)


if __name__ == '__main__':
    tic = time.perf_counter()
    asyncio.run(main())
    tac = time.perf_counter()
    print(f"{tac - tic:0.4f} seconds")
