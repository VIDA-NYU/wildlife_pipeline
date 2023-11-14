classifier_target_labels = ["a real animal",
                            "a toy",
                            "a print of an animal",
                            "an object",
                            "a faux animal",
                            "an animal body part",
                            "a faux animal body part"]

classifier_hypothesis_template = 'This product advertisement is about {}.'

phrases_to_filter = [
            "Result not found",
            "No exact matches found",
            "Not found",
            "Item Not found",
            "Search results for:"
        ]

DOMAIN_SCRAPERS = ['gumtree',
                   'auctionzip',
                   '1stdibs',
                   'canadianlisted',
                   'etsy',
                   'ukclassifieds',
                   'thetaxidermystore',
                   'skullsunlimited',
                   'glacierwear',
                   'picclick',
                   'preloved',
                   'ebay',
                   'ecrater']
