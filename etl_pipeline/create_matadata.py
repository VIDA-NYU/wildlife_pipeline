from typing import Any
import pickle


def open_scrap(minio_client: Any, domain: str):
    obj = minio_client.get_obj("scrapers", "scraper_"+domain)
    scraper = pickle.load(obj)
    return scraper


def get_dict_json_ld(product):
    obj = {}
    obj["name"]= product.get("name")
    obj["description"]= product.get("description")
    obj["image"]= product.get("image")
    obj["category"]= product.get("category")
    if obj["image"] and isinstance(obj["image"], dict):
        if obj["image"].get("contentUrl"):
            obj["image"] = obj["image"].get("contentUrl")
        elif obj["image"].get("url"):
            obj["image"] = obj["image"].get("url")
        else:
            obj["image"] = str(obj["image"])
    if obj["image"] and isinstance(obj["image"], list):
        obj["image"] = obj["image"][0]
        if obj["image"] and isinstance(obj["image"], dict):
        #     obj["image"]= obj["image"].get("contentUrl", obj["image"])
            if obj["image"].get("contentUrl"):
                obj["image"] = obj["image"].get("contentUrl")
            elif obj["image"].get("url"):
                obj["image"] = obj["image"].get("url")
            else:
                obj["image"] = str(obj["image"])
    # obj["url"]= product.get("url")
    obj["production_data"]= product.get("productionDate")
    obj["category"]= product.get("category")
    if product.get("offers") and not isinstance(product.get("offers"), list):
        obj["price"]= product.get("offers").get("price")
        obj["currency"]= product.get("offers").get("priceCurrency")
        if obj["price"] is None:
            low_price = product.get("offers").get("lowPrice")
            high_price = product.get("offers").get("highPrice")
            obj["price"] = [low_price, high_price]
            if obj["price"] == [None, None]:
                obj["price"] = None
        if product.get("offers").get("seller"):
            obj["seller"]= product.get("offers").get("seller").get("name")
            obj["seller_type"]= product.get("offers").get("seller").get("@type")
            obj["seller_url"] = product.get("offers").get("seller").get("url")
            if product.get("offers").get("seller").get("address"):
                obj["location"]= product.get("offers").get("seller").get("address").get("addressCountry")
    elif isinstance(product.get("offers"), list):
        for offers in product.get("offers"):
            try:
                obj["price"] = offers.get("price")
                obj["currency"] = offers.get("priceCurrency")
                if obj["price"] is None:
                    low_price = offers.get("lowPrice")
                    high_price = offers.get("highPrice")
                    obj["price"] = [low_price, high_price]
                    if obj["price"] == [None, None]:
                        obj["price"] = None
                if offers.get("seller"):
                    obj["seller"] = offers.get("seller").get("name")
                    obj["seller_type"] = offers.get("seller").get("@type")
                    obj["seller_url"] = offers.get("seller").get("url")
                    if offers.get("seller").get("address"):
                        obj["location"] = offers.get("seller").get("address").get("addressCountry")
            except Exception:
                print(f"offers not available: {product.get('offers')}")


    return obj


def get_sintax_opengraph(metadata: dict):
    product_meta = {}
    product_meta["name"] = metadata.get("og:title")
    product_meta["description"] =  metadata.get("og:description")
    product_meta["price"] = metadata.get("product:price:amount")
    product_meta["currency"] =  metadata.get("product:price:currency")
    product_meta["category"] = metadata.get("product:category")
    product_meta["image"] = metadata.get("og:image")
    product_meta["seller_url"] = metadata.get("og:see_also")

    return product_meta


def get_sintax_dublincore(metadata: dict):
    product_meta = {}
    metas = metadata.get("elements")
    for meta in metas:
        if meta.get("name") == "description":
            if meta.get("price"):
                product_meta["name"] = meta.get("name")
                product_meta["description"]=  meta.get("content")
                product_meta["price"] = meta.get("price")
                product_meta["currency"] =  meta.get("currency")
                product_meta["category"] = meta.get("category")
    return product_meta

def get_dict_microdata(meta: dict):
    obj = {}
    obj["name"]= meta.get("name")
    obj["description"]= meta.get("description")
    obj["image"]= meta.get("image")
    obj["price"] = meta.get("price")
    if obj["image"] and isinstance(obj["image"], dict):
        if obj["image"].get("contentUrl"):
            obj["image"]= obj["image"].get("contentUrl")
        else:
            obj["image"]= str(obj["image"])
    if obj["image"] and isinstance(obj["image"], list):
        obj["image"]= obj["image"][0]
        if obj["image"] and isinstance(obj["image"], dict):
            if obj["image"].get("contentUrl"):
                obj["image"]= obj["image"].get("contentUrl")
            else:
                obj["image"]= str(obj["image"])
    obj["production_data"]= meta.get("productionDate")
    obj["category"]= meta.get("category")
    if meta.get("mainEntity"):
        main = meta.get("mainEntity")
        if main.get("offers") and isinstance(main.get("offers"), dict):
            offer = main.get("offers")
            obj["price"]= offer.get("price", obj["price"])
            obj["currency"]= offer.get("priceCurrency")
            if offer.get("seller"):
                seller = offer.get("seller")
                obj["seller"]= seller.get("name")
                obj["seller_type"]= seller.get("@type")
                if seller.get("address"):
                    address = seller.get("address")
                    obj["location"]= address.get("addressCountry")

    if meta.get("offers") and isinstance(meta.get("offers"), dict):
        offer = meta.get("offers")
        obj["price"]= offer.get("price", obj["price"])
        obj["currency"]= offer.get("priceCurrency")
        if offer.get("seller"):
            seller = offer.get("seller")
            obj["seller"]= seller.get("name")
            obj["seller_type"]= seller.get("@type")
            if seller.get("address"):
                address = seller.get("address")
                obj["location"]= address.get("addressCountry")
    if meta.get("seller"):
        seller = meta.get("seller")
        obj["seller"]= seller.get("name")
        obj["seller_type"]= seller.get("@type")
        obj["seller_url"]= seller.get("url")
        if seller.get("address"):
            address = seller.get("address")
            obj["location"]= address.get("addressCountry")
    elif meta.get("offers") and isinstance(meta.get("offers"), list):
        obj["price"]= meta.get("offers")[0].get("price", obj["price"])
        obj["currency"]= meta.get("offers")[0].get("priceCurrency")
        if meta.get("offers")[0].get("seller"):
            obj["seller"]= meta.get("offers")[0].get("seller").get("name")
            obj["seller_type"]= meta.get("offers")[0].get("seller").get("@type")
            if meta.get("offers")[0].get("seller").get("address"):
                obj["location"]= meta.get("offers")[0].get("seller").get("address").get("addressCountry")
    return obj
