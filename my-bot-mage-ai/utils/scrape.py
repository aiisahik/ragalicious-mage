from langchain_community.document_loaders import SpiderLoader
import random
from tenacity import retry, stop_after_attempt, wait_random, before_sleep_log
from langchain_community.document_loaders import SpiderLoader
import os
import logging

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(50), 
    wait=wait_random(min=5, max=15),
    before_sleep=before_sleep_log(logger, logging.DEBUG)
)
def spider_scrape(url, params=None, mode="scrape"):
    default_params = {
            "limit":1,
            "return_format":"raw",
            "request":"http",
            'proxy_enabled': False,
            'store_data': False,
            "anti_bot": True,
            "depth": 1,
            "stealth": True,
            "metadata": True,
            # "query_selector": "sitemapindex",
            # "whitelist": ["/sitemaps"],
            # "sitemap": True,
    }
    if params:
        default_params.update(params)
    loader = SpiderLoader(
        api_key=os.environ.get('SPIDER_CLOUD_API_KEY'), 
        url=url,
        mode=mode, 
        params=default_params
    )

    data = loader.load()
    return data