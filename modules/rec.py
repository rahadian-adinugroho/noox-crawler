conf = {
                "save": [
                    {
                        "tag": "meta",
                        "attr": "property",
                        "attr_val": "og:title",
                        "save_attr": "content",
                        "as": "title",
                        "type": "title"
                    },
                ],
                "ctn_container":
                {
                    "tag": "div",
                    "attr": "id",
                    "attr_val": "news",
                    "save":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "detail_text",
                        "as": "article"
                    },
                    "loc_container":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "detail_text",
                        "save":
                            {
                                "tag": "strong",
                                "attr": None,
                                "attr_val": None,
                                "as": "loc"
                            }
                    },
                    "jdl_container":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "jdl",
                        "save": [
                            {
                                "tag": "span",
                                "attr": 'class',
                                "attr_val": 'author',
                                "as": "author",
                                "type": "title"
                            },
                            {
                                "tag": "span",
                                "attr": 'class',
                                "attr_val": 'date',
                                "as": "date",
                                "type": "title"
                            }
                        ]
                    },
                },
                "image_container":
                {
                    "tag": "div",
                    "attr": "class",
                    "attr_val": "pic_artikel|media_artikel",
                    "save": [
                        {
                            "tag": "img",
                            "attr": None,
                            "attr_val": None,
                            "save_attr": "src",
                            "as": "img_url",
                        }
                    ]
                }
            }


def _find_item(obj, key):
    ret = []
    if isinstance(obj, dict):
        if key in obj:
            ret.append(obj[key])
        for i, d in obj.items():
            item = _find_item(d, key)
            if len(item) > 0:
                ret = [v for v in item if v not in ret] + ret
        return ret
    elif isinstance(obj, list):
        for d in obj:
            item = _find_item(d, key)
            if len(item) > 0:
                ret = [v for v in item if v not in ret] + ret
        return ret
    else:
        return ret

if __name__ == '__main__':
    for i, v in conf.items():
        print(i+': '+str(v))
    print(_find_item(conf, 'tag'))
