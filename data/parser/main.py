import json
from urllib.request import urlopen


def load_json():
    with open("../unogs-data.json") as f:
        data = json.load(f)
    return data


def get_full_country_list(id: int) -> dict:
    base_url = 'https://unogs.com/api/title/countries?netflixid='
    url = base_url + str(id)
    response = urlopen(url)
    return json.loads(response.read())


def main():
    unogs_data = load_json()
    result = []
    total_count = len(unogs_data['results'])
    i = 0
    for movie in unogs_data['results']:
        netflix_id = movie['nfid']
        countries = get_full_country_list(netflix_id)
        movie["countries"] = countries
        result.append(movie)
        i += 1
        if i % 100 == 0:
            print(f"Downloaded {i} items, total number of items: {total_count}")
    return result


def test():
    all_data = load_json()["results"]

    pp = filter(lambda x: x["nfid"] == 81409000, all_data)
    q = list(pp)[0]

    qq = eval('{' + q["clist"] + '}')
    cc = qq.values()
    print(sorted(cc))
    print(sorted([x['country'] for x in get_full_country_list(81409000)]))


def write_to_file(json_file: dict):
    json_object = json.dumps(json_file, indent=4)
    with open("out.json", "w") as outfile:
        outfile.write(json_object)


if __name__ == '__main__':
    print("Starting downloading data...")
    r = main()
    print("Completed downloading data!")
    print("Starting writing output to the json file...")
    write_to_file(r)
    print("Writing to json file has been completed!")
