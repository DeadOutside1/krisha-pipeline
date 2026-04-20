import scrapy
import json
import re
from datetime import datetime



class ListingsSpider(scrapy.Spider):
    name = "listings"
    cities = {
        "almaty":"https://krisha.kz/arenda/kvartiry/almaty/",
        #"astana":"https://krisha.kz/arenda/kvartiry/astana/",
        #"shymkent":"https://krisha.kz/arenda/kvartiry/shymkent/",
    }

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": True,

    }

    def start_requests(self):
        for city, url in self.cities.items():
            yield scrapy.Request(url=url, callback=self.parse, meta={"city": city, "page": 1}
                                 )

    def parse_date(self, date_str):
        """Конвертируем '20 апр.' в datetime.date"""
        months = {
            "янв": 1, "фев": 2, "мар": 3, "апр": 4,
            "май": 5, "июн": 6, "июл": 7, "авг": 8,
            "сен": 9, "окт": 10, "ноя": 11, "дек": 12
        }
        match = re.search(r"(\d+)\s+([а-я]+)", date_str.lower())
        if match:
            day = int(match.group(1))
            month = months.get(match.group(2)[:3])
            if month:
                return datetime(datetime.now().year, month, day).date()
        return None

    def parse(self, response):
        city = response.meta["city"]
        page = response.meta["page"]
        today = datetime.utcnow().date()
        stop_crawling = False

        for listing in response.css("div.a-card"):
            stats = listing.css("div.a-card__stats-item::text").getall()
            date_str = str(stats[1]).strip() if len(stats) > 1 else ""
            pub_date = self.parse_date(date_str)

            # Если дата старше сегодня — стопаем
            if pub_date and pub_date < today:
                stop_crawling = True
                break

            yield {
                "listing_id": listing.css("::attr(data-id)").get(),
                "city": city,
                "title": listing.css("a.a-card__title::text").get("").strip(),
                "price": listing.css("div.a-card__price::text").get("").strip(),
                "address": listing.css("div.a-card__subtitle::text").get("").strip(),
                "url": listing.css("a.a-card__title::attr(href)").get().strip(),
                "date_publicated": date_str,
                "scraped_at": datetime.utcnow().isoformat(),
            }

        # Идём на следующую страницу только если не встретили старые объявления
        next_page = response.css("a.paginator-btn-next::attr(href)").get()
        if next_page and not stop_crawling:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={"city": city, "page": page + 1}
            )