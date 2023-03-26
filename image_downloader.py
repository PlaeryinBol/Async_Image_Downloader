import argparse
import asyncio
import concurrent.futures
import glob
import logging
import math
import os
import random
import secrets
import time
from io import BytesIO

import aiofiles
import aiohttp
import pandas as pd
import requests
from PIL import Image
from tqdm.asyncio import tqdm


def make_folders(n):
    """Create a subfolder structure in out folder - otherwise, a lot of files in one folder will lead to a freeze work.

        Args:
            n (int): folders count.
        Returns:
            -
    """
    os.makedirs(opt.out_dir, exist_ok=True)
    for i in range(folder_index, folder_index + n):
        new_folder = opt.out_dir + "/folder_" + str(i)
        os.makedirs(new_folder, exist_ok=True)


def img_resize(img, target_size, original_ratio=True):
    """Image resizing according to given conditions.

        Args:
            img (PIL.image): original image.
            target_size (tuple): desired aspect ratio for the saving image (like "(1200, 900)", width first).
            original_ratio (bool): flag, whether to keep the original aspect ratio, defaults to True.
        Returns:
            img (PIL.image): resized image.
    """
    # if the image does not need to be resized
    if target_size is None:
        pass
    # if the width of the original image is greater than the target width
    if img.size[1] > target_size[0]:
        # keep the original image aspect ratio
        if original_ratio:
            wpercent = (target_size[0]/float(img.size[0]))
            hsize = int((float(img.size[1])*float(wpercent)))
        else:
            hsize = target_size[1]
        img = img.resize((target_size[0], hsize), Image.ANTIALIAS)
    # if the height of the original image is greater than the target height
    elif img.size[0] > target_size[1]:
        # keep the original image aspect ratio
        if original_ratio:
            hpercent = (target_size[1]/float(img.size[1]))
            wsize = int((float(img.size[0])*float(hpercent)))
        else:
            wsize = target_size[0]
        img = img.resize((wsize, target_size[1]), Image.ANTIALIAS)
    else:
        img = img.resize(target_size, Image.ANTIALIAS)

    return img


def standart_loop(df):
    """Image downloading with a standard python loop.

        Args:
            df (pandas.DataFrame): dataframe with images urls and names.
        Returns:
            -
    """
    global count
    global folder_index

    for idx, name in enumerate(tqdm(df['names'])):
        filename = str(name) + '.jpg'
        url = df['urls'][idx]
        try:
            response = requests.request("GET", url, headers=HEADERS, timeout=opt.response_timeout, proxies=PROXIES)
            # we can check that the status code is 200 before doing anything else
            if response.status_code != 200:
                LOG.error(f"{str(url)} {response.status}")
                continue

            image_data = response.content
            img = Image.open(BytesIO(image_data)).convert('RGB')
            # if necessary, skip too small images
            if opt.skip_small and ((img.size[0] < opt.target_size[0]) or (img.size[1] < opt.target_size[1])):
                LOG.info(f"Skipped: {url} - too small")
                continue

            img = img_resize(img, opt.target_size, opt.original_ratio)
        except Exception as e:
            LOG.error(f"{str(url)}: {e}")
            continue

        # if the current folder is full, go to the next one
        if count >= opt.folder_size:
            count = 0
            folder_index += 1
            LOG.info('Saved ~{} images'.format(folder_index * opt.folder_size))
        else:
            count += 1

        inner_path = opt.out_dir + "/folder_" + str(folder_index)
        save_path = os.path.join(inner_path, filename)
        img.save(save_path, format='JPEG', quality=opt.quality)
        # if necessary, wait for some random time in order to avoid a ban
        if opt.inter_timeouts:
            time.sleep(random.randint(*opt.inter_timeouts))


def img_save(out_dir, filename, img):
    """Saving single image to the folder.

        Args:
            out_dir (str): top-level images folder.
            filename (str): name for the image.
            img (PIL.image): image to save.
        Returns:
            -
    """
    global count
    global folder_index

    inner_path = out_dir + "/folder_" + str(folder_index)
    save_path = os.path.join(inner_path, filename)
    # save the image to the current folder, if it is not filled yet
    if count < opt.folder_size:
        img.save(save_path, format='JPEG', quality=opt.quality)
        count += 1
    else:
        count = 0
        folder_index += 1
        img_save(out_dir, filename, img)


def download_image(row):
    """Downloading single image from dataframe.

        Args:
            row (tuple): (image name, image url) tuple.
        Returns:
            -
    """
    filename = str(row[0]) + '.jpg'
    url = row[1]
    try:
        response = requests.request("GET", url, headers=HEADERS, timeout=opt.response_timeout, proxies=PROXIES)
        # we can check that the status code is 200 before doing anything else
        if response.status_code != 200:
            LOG.error(f"{str(url)} {response.status}")
            return

        image_data = response.content
        img = Image.open(BytesIO(image_data)).convert('RGB')
        # if necessary, skip too small images
        if opt.skip_small and ((img.size[0] < opt.target_size[0]) or (img.size[1] < opt.target_size[1])):
            LOG.info(f"Skipped: {url} - too small")
            return

        img = img_resize(img, opt.target_size, opt.original_ratio)
        img_save(opt.out_dir, filename, img)
        # if necessary, wait for some random time in order to avoid a ban
        if opt.inter_timeouts:
            time.sleep(random.randint(*opt.inter_timeouts))
    except Exception as e:
        LOG.error(f"{str(url)}: {e}")
    else:
        LOG.info(f"Downloaded: {url}")


def threads_download(df, max_workers=32):
    """Downloading images with multithreading.

        Args:
            df (pandas.DataFrame): dataframe with images urls and names.
            max_workers (int): maximum number of threads, default to 32.
        Returns:
            -
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        list(tqdm(executor.map(download_image, df[['names', 'urls']].apply(tuple, axis=1)), total=len(df)))


def processes_download(df, max_workers=32):
    """Downloading images with multiprocessing.

        Args:
            df (pandas.DataFrame): dataframe with images urls and names.
            max_workers (int): maximum number of processes, default to 32.
        Returns:
            -
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers) as executor:
        list(tqdm(executor.map(download_image, df[['names', 'urls']].apply(tuple, axis=1)), total=len(df)))


async def async_download_image(session, df, idx):
    """Asynchronous downloading single image from dataframe.

        Args:
            session (aiohttp.ClientSession): client session for making HTTP requests.
            df (pandas.DataFrame): dataframe with images urls and names.
            idx (int): dataframe row index.
        Returns:
            -
    """
    filename = str(df['names'][idx]) + '.jpg'
    url = df['urls'][idx]
    try:
        async with session.get(url, timeout=opt.response_timeout, proxy=PROXIES) as resp:
            global count
            global folder_index
            # we can check that the status code is 200 before doing anything else
            if resp.status != 200:
                LOG.error(f"{str(url)} {resp.status}")
                return

            # save the image to the current folder, if it is not filled yet
            if count < opt.folder_size - 1:
                inner_path = opt.out_dir + "/folder_" + str(folder_index)
                save_path = os.path.join(inner_path, filename)
                request_object_content = await resp.read()
                img = Image.open(BytesIO(request_object_content)).convert('RGB')
                # if necessary, skip too small images
                if opt.skip_small and ((img.size[0] < opt.target_size[0]) or (img.size[1] < opt.target_size[1])):
                    LOG.info(f"Skipped: {url} - too small")
                    return

                img = img_resize(img, opt.target_size, opt.original_ratio)
                buf = BytesIO()
                img.save(buf, format='JPEG')
                async with aiofiles.open(save_path, 'wb') as out_file:
                    await out_file.write(buf.getbuffer())
                count += 1
                # if necessary, wait for some random time in order to avoid a ban
                if opt.inter_timeouts:
                    time.sleep(random.randint(*opt.inter_timeouts))
            else:
                count = 0
                folder_index += 1
                async_download_image(session, df, idx)
    except Exception as e:
        LOG.error(f"{str(url)}: {e}")
    else:
        LOG.info(f"Downloaded: {url}")


async def async_download(df):
    """Asynchronous images downloading.

        Args:
            df (pandas.DataFrame): dataframe with images urls and names.
        Returns:
            -
    """
    async with aiohttp.ClientSession(trust_env=True, headers=HEADERS) as session:
        tasks = [async_download_image(session, df, idx) for idx in range(len(df))]
        _ = [await task_ for task_ in tqdm.as_completed(tasks, total=len(tasks))]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--df', type=str, default='image_urls.tsv', help='dataframe with image urls')
    parser.add_argument('--slice', type=int, default=None, help='slice to limit size of dataframe')
    parser.add_argument('--name_format', type=str, choices={'by_df', 'by_url', 'random'},
                        default='by_df', help='format for the name of the downloaded images')
    parser.add_argument('--out_dir', type=str, default='images', help='folder for downloaded images')
    parser.add_argument('--mode', type=str, choices={'loop', 'threads', 'processes', 'async'},
                        default='async', help='mode for downloading')
    parser.add_argument('--folder_size', type=int, default=5000, help='max images in inner folder')
    parser.add_argument('--folder_index', type=int, default=0, help='index of first inner folder for saving')
    parser.add_argument('--response_timeout', type=int, default=10, help='timeout for response')
    parser.add_argument('--target_size', type=tuple, default=(256, 256), help='image size for saving process')
    parser.add_argument('--original_ratio', type=bool, default=True, help='keep the original aspect ratio of the image')
    parser.add_argument('--skip_small', type=bool, default=True, help='skip images whose side < target_size')
    parser.add_argument('--quality', type=int, default=95, help='image quality for saving process')
    parser.add_argument('--inter_timeouts', type=tuple, default=None,
                        help='interval (like "(1, 4)") for sampling random timeout after downloading each image')
    parser.add_argument('--logs', type=str, default='logs.log', help='log file')
    opt = parser.parse_args()

    # initialize logging
    logging.basicConfig(filename=opt.logs, level=logging.INFO,
                        filemode="w", format='%(asctime)s: %(levelname)s - %(message)s')
    LOG = logging.getLogger()
    LOG.info(f"Mode: {opt.mode}")

    df = pd.read_table(opt.df)
    # if necessary, take only the specified number of dataframe elements
    if opt.slice:
        df = df[:opt.slice]

    # if there are only urls in the dataframe
    if 'names' not in df.columns:
        # if images need to be named the same as their urls
        if opt.name_format == 'by_url':
            df['names'] = [url.split('/')[-1].split('.')[0] for url in df['urls']]
        # if images need to be named by random symbols
        elif opt.name_format == 'random':
            df['names'] = [secrets.token_hex(nbytes=8) for _ in range(len(df['urls']))]

    count = 0
    folder_index = opt.folder_index
    # create subfolders for images
    n_folders = math.ceil(len(df) / opt.folder_size)
    make_folders(n_folders)

    # here you can enter your own headers and proxies if needed
    HEADERS = None
    PROXIES = None

    start = time.time()
    # apply the download algorithm depending on the mode
    if opt.mode == 'loop':
        standart_loop(df)
    elif opt.mode == 'threads':
        threads_download(df)
    elif opt.mode == 'processes':
        processes_download(df)
    elif opt.mode == 'async':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_download(df))

    LOG.info(f"Elapsed time: {str(time.time() - start)}")
    count_images = len(glob.glob(opt.out_dir + '/**/*.jpg', recursive=True))

    # delete empty subfolders
    for folder in list(os.walk(opt.out_dir))[1:]:
        if not folder[2]:
            os.rmdir(folder[0])

    LOG.info(f"{count_images} downloaded images in output folder")
